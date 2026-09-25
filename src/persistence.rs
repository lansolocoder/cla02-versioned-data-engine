//! 持久化与崩溃恢复。
//!
//! 磁盘布局：持久化根目录下每个已提交版本对应一个独立条目文件，
//! 文件名为 20 位零填充十进制版本号加 `.vde` 后缀（例如
//! `00000000000000000001.vde`），因此字典序就是版本提交顺序。
//!
//! 单个条目文件的帧格式为：
//!
//! ```text
//! VDE1\n
//! <20 位十进制载荷长度>\n
//! <JSON 载荷：版本号、快照内容、提交时刻数据集内容>
//! \n
//! <8 位小写十六进制 CRC32（IEEE，校验范围为载荷字节）>\n
//! VDE-END-1\n
//! ```
//!
//! 提交采用“同目录临时文件 + fsync + 原子 rename”：rename 之前崩溃在磁盘上
//! 只可能留下点号前缀的临时文件（启动时清除），不可能出现最终文件名的半写
//! 条目；rename 之后整条目要么完整可见、要么不存在。每个条目都带有长度、
//! 边界魔数与校验和，可独立判定完整性，因此中间条目损坏时可以只跳过它而
//! 继续恢复其后完整的条目。

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// 当前数据集：与 `main` 中的类型同构，重复定义以让持久化模块自包含。
pub type Dataset = BTreeMap<String, Map<String, Value>>;
/// 一份快照的记录：键 -> 字段集 JSON 对象。
pub type Records = BTreeMap<String, Value>;

const MAGIC: &[u8] = b"VDE1\n";
const TAIL_MAGIC: &[u8] = b"VDE-END-1\n";
/// 版本号固定 20 位（u64 上限的位数），保证文件名字典序即数值序。
const VERSION_WIDTH: usize = 20;
const ENTRY_SUFFIX: &str = ".vde";
/// 头部：起始魔数 + 20 位长度 + '\n'。
const HEADER_LEN: usize = MAGIC.len() + VERSION_WIDTH + 1;
/// 尾部：'\n' + 8 位 CRC32 + '\n' + 结束魔数。
const TAIL_LEN: usize = 1 + 8 + 1 + TAIL_MAGIC.len();

/// 落盘条目：版本记录、快照内容与提交时刻的数据集内容一起原子写入。
/// 保存时刻快照就是数据集的深拷贝，两个字段内容相同但语义独立——
/// 快照不可变，而数据集字段用于重启后恢复当前工作集。
#[derive(Serialize, Deserialize)]
struct EntryOnDisk {
    version: u64,
    snapshot: Dataset,
    dataset: Dataset,
}

/// 只用于序列化的借用形态，避免保存时把数据集克隆两份。
#[derive(Serialize)]
struct EntryWire<'a> {
    version: u64,
    snapshot: &'a Dataset,
    dataset: &'a Dataset,
}

/// 恢复出的一份快照。
pub struct LoadedSnapshot {
    pub version: u64,
    pub records: Records,
}

/// 启动加载的结果。
pub struct Loaded {
    pub snapshots: Vec<LoadedSnapshot>,
    /// 当前工作集：最后一个完整条目提交时的数据集。
    pub dataset: Dataset,
    /// 下一个可分配版本号（无任何完整条目时为 1）。
    pub next_version: u64,
}

/// 标准 IEEE CRC32（与 zlib 同一多项式 0xEDB88320），编译期生成查表。
mod crc32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0usize;
        while i < 256 {
            let mut c = i as u32;
            let mut j = 0;
            while j < 8 {
                c = if c & 1 != 0 {
                    0xEDB88320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                j += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };

    pub fn checksum(data: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &b in data {
            crc = TABLE[((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8);
        }
        !crc
    }
}

/// 把载荷按帧格式打包成完整文件字节。
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + TAIL_LEN);
    out.extend_from_slice(MAGIC);
    writeln!(&mut out, "{:0width$}", payload.len(), width = VERSION_WIDTH).unwrap();
    out.extend_from_slice(payload);
    out.push(b'\n');
    writeln!(&mut out, "{:08x}", crc32::checksum(payload)).unwrap();
    out.extend_from_slice(TAIL_MAGIC);
    out
}

/// 校验帧边界、长度与 CRC32；通过则返回载荷切片。任何不符都返回 None，
/// 调用方据此把整个条目按“不存在”处理。
fn unframe(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < HEADER_LEN + TAIL_LEN {
        return None;
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return None;
    }

    let len_start = MAGIC.len();
    let len_end = len_start + VERSION_WIDTH;
    let len_digits = &bytes[len_start..len_end];
    if !len_digits.iter().all(|b| b.is_ascii_digit()) || bytes[len_end] != b'\n' {
        return None;
    }
    let len_str = std::str::from_utf8(len_digits).ok()?;
    let payload_len: usize = len_str.parse().ok()?;

    // 总长必须精确匹配：拒绝截断，也拒绝尾部多余字节。
    if bytes.len() != HEADER_LEN + payload_len + TAIL_LEN {
        return None;
    }

    let payload = &bytes[HEADER_LEN..HEADER_LEN + payload_len];
    let mut cursor = HEADER_LEN + payload_len;
    if bytes[cursor] != b'\n' {
        return None;
    }
    cursor += 1;
    let crc_hex = &bytes[cursor..cursor + 8];
    cursor += 8;
    if bytes[cursor] != b'\n' {
        return None;
    }
    cursor += 1;
    if &bytes[cursor..] != TAIL_MAGIC {
        return None;
    }

    let stored = u32::from_str_radix(std::str::from_utf8(crc_hex).ok()?, 16).ok()?;
    if stored != crc32::checksum(payload) {
        return None;
    }
    Some(payload)
}

fn entry_name(version: u64) -> String {
    format!("{version:0width$}{ENTRY_SUFFIX}", width = VERSION_WIDTH)
}

/// 严格匹配条目文件名并取出版本号；不符合命名规则的名字一律不是条目。
fn version_from_name(name: &str) -> Option<u64> {
    if name.len() != VERSION_WIDTH + ENTRY_SUFFIX.len() || !name.ends_with(ENTRY_SUFFIX) {
        return None;
    }
    let digits = &name[..VERSION_WIDTH];
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// 原子写入一个版本条目：版本记录、快照与数据集内容同生共死。
///
/// 临时文件与最终文件位于同一目录（同一文件系统），先写全并 fsync，再
/// rename——rename 在同一文件系统上是原子的。提交点（rename）之前的
/// 任何失败都会清理临时文件，磁盘上不留下本次保存的痕迹；返回 Err 时
/// 调用方必须保持内存状态（数据集、版本号）不变。
pub fn write_entry(dir: &Path, version: u64, dataset: &Dataset) -> io::Result<()> {
    let wire = EntryWire {
        version,
        snapshot: dataset,
        dataset,
    };
    let payload =
        serde_json::to_vec(&wire).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let bytes = frame(&payload);

    let final_path = dir.join(entry_name(version));
    let tmp_name = format!(
        ".{}.tmp-{}-{}",
        entry_name(version),
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let tmp_path = dir.join(tmp_name);

    let outcome = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    })();

    match &outcome {
        Ok(()) => {
            // 让目录项变更落盘；失败不影响 rename 已经原子可见这一事实。
            if let Ok(dir_file) = File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }
        Err(_) => {
            // 绝不让失败保存留下任何临时痕迹。
            let _ = fs::remove_file(&tmp_path);
        }
    }
    outcome
}

/// 从持久化目录恢复状态。
///
/// 条目按文件名（即持久化顺序）排序后逐个独立校验：半写、截断、校验不
/// 通过、JSON 无法解析、版本号与文件名不符或非严格递增的条目一律按不
/// 存在跳过，但不影响其后完整条目恢复。当前数据集取最后一个完整条目所
/// 记录的内容，之后的数据一律丢弃；下一版本号为最后恢复版本加 1。
/// 目录里没有任何完整条目时按全新实例启动（版本号从 1 开始）。
pub fn load(dir: &Path) -> io::Result<Loaded> {
    let mut entry_names: Vec<String> = Vec::new();

    for entry in fs::read_dir(dir)? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();

        // 上次崩溃遗留的临时文件：rename 从未发生，即该版本从未提交，
        // 直接清除；无法删除也不影响启动，读取时会被命名规则忽略。
        if name.starts_with('.') && name.contains(".vde.tmp-") {
            let _ = fs::remove_file(entry.path());
            continue;
        }
        if version_from_name(&name).is_some() {
            entry_names.push(name);
        }
        // 其他名字的文件不是本服务的持久化条目，原样保留、不予理会。
    }

    entry_names.sort();

    let mut snapshots = Vec::new();
    let mut dataset = Dataset::new();
    let mut last_version: Option<u64> = None;
    let mut previous_version: Option<u64> = None;

    for name in &entry_names {
        let name_version = version_from_name(name).expect("已按命名规则筛选");
        let raw = match fs::read(PathBuf::from(dir).join(name)) {
            Ok(bytes) => bytes,
            Err(err) => {
                eprintln!("持久化条目 {name} 无法读取，已跳过: {err}");
                continue;
            }
        };

        let Some(payload) = unframe(&raw) else {
            eprintln!("持久化条目 {name} 写入不完整或校验失败，已跳过");
            continue;
        };
        let parsed: EntryOnDisk = match serde_json::from_slice(payload) {
            Ok(entry) => entry,
            Err(err) => {
                eprintln!("持久化条目 {name} 内容无法解析，已跳过: {err}");
                continue;
            }
        };

        if parsed.version == 0
            || parsed.version != name_version
            || previous_version.is_some_and(|prev| parsed.version <= prev)
        {
            eprintln!("持久化条目 {name} 版本号异常，已跳过");
            continue;
        }

        previous_version = Some(parsed.version);
        last_version = Some(parsed.version);
        dataset = parsed.dataset;
        let records: Records = parsed
            .snapshot
            .into_iter()
            .map(|(key, fields)| (key, Value::Object(fields)))
            .collect();
        snapshots.push(LoadedSnapshot {
            version: parsed.version,
            records,
        });
    }

    Ok(Loaded {
        snapshots,
        dataset,
        next_version: last_version.map(|v| v + 1).unwrap_or(1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "vde-persist-test-{}-{}",
            std::process::id(),
            TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn dataset_of(pairs: &[(&str, u64)]) -> Dataset {
        pairs
            .iter()
            .map(|(key, n)| {
                (
                    (*key).to_owned(),
                    Map::from_iter([("n".to_owned(), Value::from(*n))]),
                )
            })
            .collect()
    }

    #[test]
    fn fresh_dir_starts_at_version_one() {
        let dir = temp_dir();
        let loaded = load(&dir).unwrap();
        assert_eq!(loaded.next_version, 1);
        assert!(loaded.snapshots.is_empty());
        assert!(loaded.dataset.is_empty());
    }

    #[test]
    fn frame_roundtrip_and_corruption() {
        let payload = b"{\"version\":1,\"x\":true}";
        let framed = frame(payload);
        assert_eq!(unframe(&framed).unwrap(), payload);

        // 截断：尾部不完整。
        assert!(unframe(&framed[..framed.len() - 3]).is_none());
        // 多余字节。
        let mut extra = framed.clone();
        extra.push(0);
        assert!(unframe(&extra).is_none());
        // 翻转载荷中的一个字节：CRC 不匹配。
        let mut flipped = framed.clone();
        flipped[HEADER_LEN + 2] ^= 0xff;
        assert!(unframe(&flipped).is_none());
        // 起始魔数损坏。
        let mut bad_magic = framed.clone();
        bad_magic[0] = b'X';
        assert!(unframe(&bad_magic).is_none());
    }

    #[test]
    fn restart_recovers_entries_and_continues_version() {
        let dir = temp_dir();
        let d1 = dataset_of(&[("alpha", 1)]);
        let d2 = dataset_of(&[("alpha", 1), ("bravo", 2)]);
        write_entry(&dir, 1, &d1).unwrap();
        write_entry(&dir, 2, &d2).unwrap();

        let loaded = load(&dir).unwrap();
        assert_eq!(loaded.next_version, 3);
        assert_eq!(loaded.snapshots.len(), 2);
        assert_eq!(loaded.snapshots[0].version, 1);
        assert_eq!(loaded.snapshots[1].version, 2);
        assert_eq!(
            loaded.dataset.get("bravo").unwrap().get("n").unwrap(),
            &Value::from(2)
        );
    }

    #[test]
    fn corrupt_middle_entry_skipped_but_later_entries_recover() {
        let dir = temp_dir();
        write_entry(&dir, 1, &dataset_of(&[("alpha", 1)])).unwrap();
        write_entry(&dir, 2, &dataset_of(&[("alpha", 2)])).unwrap();
        write_entry(&dir, 3, &dataset_of(&[("alpha", 3)])).unwrap();

        // 把版本 2 的文件中载荷字节翻转：CRC 校验失败。
        let v2 = dir.join(entry_name(2));
        let mut bytes = fs::read(&v2).unwrap();
        bytes[HEADER_LEN + 5] ^= 0x01;
        fs::write(&v2, &bytes).unwrap();

        let loaded = load(&dir).unwrap();
        let versions: Vec<u64> = loaded.snapshots.iter().map(|s| s.version).collect();
        assert_eq!(versions, vec![1, 3]); // 损坏条目跳过，其后的 3 仍然恢复
        assert_eq!(loaded.next_version, 4); // 从已恢复的最大版本号继续
        assert_eq!(
            loaded.dataset.get("alpha").unwrap().get("n").unwrap(),
            &Value::from(3)
        );

        // 半写的最后条目：v3 被截断，前缀恢复到 v2。
        let dir2 = temp_dir();
        write_entry(&dir2, 1, &dataset_of(&[("k", 1)])).unwrap();
        write_entry(&dir2, 2, &dataset_of(&[("k", 2)])).unwrap();
        write_entry(&dir2, 3, &dataset_of(&[("k", 3)])).unwrap();
        let v3 = dir2.join(entry_name(3));
        let bytes = fs::read(&v3).unwrap();
        fs::write(&v3, &bytes[..HEADER_LEN + 4]).unwrap();
        let loaded2 = load(&dir2).unwrap();
        let versions: Vec<u64> = loaded2.snapshots.iter().map(|s| s.version).collect();
        assert_eq!(versions, vec![1, 2]);
        assert_eq!(loaded2.next_version, 3); // 半写版本号被复用
    }

    #[test]
    fn stale_temp_files_are_removed_and_never_commit() {
        let dir = temp_dir();
        write_entry(&dir, 1, &dataset_of(&[("alpha", 1)])).unwrap();

        // 模拟崩溃遗留：为版本 2 写了完整临时文件但从未 rename。
        let payload = serde_json::to_vec(&EntryWire {
            version: 2,
            snapshot: &dataset_of(&[("alpha", 9)]),
            dataset: &dataset_of(&[("alpha", 9)]),
        })
        .unwrap();
        let tmp = dir.join(format!(
            ".{}.tmp-{}-{}",
            entry_name(2),
            std::process::id(),
            999
        ));
        fs::write(&tmp, frame(&payload)).unwrap();

        let loaded = load(&dir).unwrap();
        assert_eq!(loaded.snapshots.len(), 1);
        assert_eq!(loaded.next_version, 2);
        assert!(!tmp.exists()); // 遗留临时文件被清理
    }

    #[test]
    fn failed_write_leaves_no_trace() {
        let dir = temp_dir();
        write_entry(&dir, 1, &dataset_of(&[("alpha", 1)])).unwrap();

        // 只读目录：新建临时文件失败。
        let readonly = temp_dir();
        let mut perms = fs::metadata(&readonly).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&readonly, perms).unwrap();

        let result = write_entry(&readonly, 1, &dataset_of(&[("x", 1)]));
        assert!(result.is_err());
        let leftovers: Vec<_> = fs::read_dir(&readonly)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(leftovers.is_empty(), "失败保存不得留下任何文件");
    }
}
