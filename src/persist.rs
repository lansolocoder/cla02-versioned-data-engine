//! 基于文件的崩溃安全持久化。
//!
//! 磁盘布局（VDE_DATA_DIR 下）：
//!
//! ```text
//! lock                         进程持有的 flock（进程退出即由内核释放）
//! writes/00000000000000000001.json   每次成功写入批次一个不可变事件文件
//! snapshots/00000000000000000001.json 每次成功保存版本一个完整快照文件
//! ```
//!
//! 提交协议：任何事件都先写入同目录下的临时文件，fsync 文件后 `rename` 为
//! 最终文件名，再 fsync 目录。rename 在同一文件系统内是原子的，因此一份快照
//! 要么以「完整且校验通过」的最终文件存在，要么完全不存在，不会出现半截快照。
//! 快照文件本身就是提交标记，不需要额外的 commit 记录。
//!
//! 文件格式：首行 `VDE1 <payload 字节数> <payload 的 CRC32-IEEE,8 位十六进制>`，
//! 其后紧跟恰好那么多字节的 JSON payload。恢复时任何长度、校验和或解析错误都
//! 视为数据损坏并拒绝启动。

use crate::{Dataset, Snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// 持久化运行时状态，随 [`crate::Store`] 一起存活，持有的锁文件描述符在进程
/// 退出（包括被强杀）时由内核自动释放。
pub(crate) struct Persist {
    root: PathBuf,
    /// 下一个写入事件的全局序号。已持久化的写入事件序号为 1..=next_seq-1。
    next_seq: u64,
    /// 同一进程内临时文件名的单调计数，避免任何重名。
    tmp_counter: u64,
    _lock: File,
}

#[derive(Serialize, Deserialize)]
struct DiskRecord {
    key: String,
    fields: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum EventPayload {
    Write {
        seq: u64,
        records: Vec<DiskRecord>,
    },
    Snapshot {
        version: u64,
        /// 该快照保存时已持久化的最后一个写入事件序号（没有任何写入时为 0）。
        last_seq: u64,
        records: Vec<DiskRecord>,
    },
}

/// 启动期错误：要么目录被占用，要么数据损坏/不可访问。三种情况都拒绝启动。
#[derive(Debug)]
pub(crate) enum OpenError {
    Locked { root: PathBuf },
    Corrupt(String),
    Io(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Locked { root } => write!(
                f,
                "持久化目录 {} 已被另一个运行中的服务实例占用；多个进程不能共用同一持久化目录",
                root.display()
            ),
            OpenError::Corrupt(message) => {
                write!(f, "持久化数据损坏或无法解析，拒绝启动: {message}")
            }
            OpenError::Io(message) => write!(f, "无法访问持久化目录: {message}"),
        }
    }
}

impl std::error::Error for OpenError {}

fn io_err(context: impl Into<String>) -> impl FnOnce(io::Error) -> OpenError {
    let context = context.into();
    move |e| OpenError::Io(format!("{context}: {e}"))
}

// ---------------------------------------------------------------------------
// 启动与恢复
// ---------------------------------------------------------------------------

/// 打开（必要时创建）持久化目录并恢复出与上次进程完全一致的内存状态。
///
/// 返回 (当前数据集, 全部历史快照, 持久化句柄)。任何损坏/占用都返回错误，
/// 调用方负责写 stderr 并非零退出，此函数不会部分加载或覆盖任何文件。
pub(crate) fn open(root: PathBuf) -> Result<(Dataset, Vec<Snapshot>, Persist), OpenError> {
    // 1. 目录必须已存在或可创建。create_dir_all 对已存在目录不做任何改动。
    fs::create_dir_all(&root).map_err(io_err(format!("创建目录 {}", root.display())))?;

    // 2. 抢目录锁。抢锁失败说明有另一个存活实例：立即返回，不动任何数据。
    let lock = acquire_lock(&root)?;

    // 3. 子目录与目录项的创建也要在持锁后进行并 fsync，自身崩溃后状态确定。
    let writes_dir = root.join("writes");
    let snapshots_dir = root.join("snapshots");
    fs::create_dir_all(&writes_dir)
        .map_err(io_err(format!("创建目录 {}", writes_dir.display())))?;
    fs::create_dir_all(&snapshots_dir)
        .map_err(io_err(format!("创建目录 {}", snapshots_dir.display())))?;
    sync_dir(&root).map_err(io_err(format!("同步目录 {}", root.display())))?;

    // 4. 清理上次进程在 rename 前崩溃残留的临时文件（它们从未提交，删除安全）。
    clean_tmp(&writes_dir)?;
    clean_tmp(&snapshots_dir)?;

    // 5. 读取并严格校验全部事件，重放出数据集与快照。
    let writes = load_writes(&writes_dir)?;
    let snapshots = load_snapshots(&snapshots_dir)?;
    let (dataset, snapshots, max_seq) = replay(writes, snapshots)?;

    Ok((
        dataset,
        snapshots,
        Persist {
            root,
            next_seq: max_seq + 1,
            tmp_counter: 0,
            _lock: lock,
        },
    ))
}

/// 对 lock 文件加排他、非阻塞 flock。
fn acquire_lock(root: &Path) -> Result<File, OpenError> {
    let path = root.join("lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(io_err(format!("打开锁文件 {}", path.display())))?;

    // 直接声明 libc 符号，避免引入额外依赖。macOS/Linux 均默认链接 libc。
    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    // macOS EWOULDBLOCK=35；Linux EAGAIN/EWOULDBLOCK=11。
    const E_AGAIN_OR_WOULD_BLOCK: &[Option<i32>] = &[Some(35), Some(11)];

    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        Ok(file)
    } else {
        let error = io::Error::last_os_error();
        if E_AGAIN_OR_WOULD_BLOCK.contains(&error.raw_os_error()) {
            Err(OpenError::Locked {
                root: root.to_path_buf(),
            })
        } else {
            Err(OpenError::Io(format!(
                "对 {} 加锁失败: {error}",
                path.display()
            )))
        }
    }
}

/// 删除目录内自身崩溃残留的临时文件。
fn clean_tmp(dir: &Path) -> Result<(), OpenError> {
    for entry in fs::read_dir(dir).map_err(io_err(format!("读取目录 {}", dir.display())))? {
        let entry = entry.map_err(io_err(format!("读取目录 {}", dir.display())))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".tmp.") {
            fs::remove_file(entry.path()).map_err(io_err(format!(
                "删除残留临时文件 {}",
                entry.path().display()
            )))?;
        }
    }
    Ok(())
}

/// 列出目录中形如 `00000000000000000001.json` 的文件；其它文件名一律视为损坏。
fn numbered_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, OpenError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_err(format!("读取目录 {}", dir.display())))? {
        let entry = entry.map_err(io_err(format!("读取目录 {}", dir.display())))?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        match parse_numbered(&name) {
            Some(number) => found.push((number, path)),
            None => {
                return Err(OpenError::Corrupt(format!(
                    "目录 {} 中存在无法识别的数据文件 {name}",
                    dir.display()
                )));
            }
        }
    }
    Ok(found)
}

fn parse_numbered(name: &str) -> Option<u64> {
    const DIGITS: usize = 20;
    let digits = name.strip_suffix(".json")?;
    if digits.len() != DIGITS || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn load_writes(dir: &Path) -> Result<BTreeMap<u64, Vec<DiskRecord>>, OpenError> {
    let mut events = BTreeMap::new();
    for (seq, path) in numbered_files(dir)? {
        let EventPayload::Write {
            seq: payload_seq,
            records,
        } = read_event(&path)?
        else {
            return Err(OpenError::Corrupt(format!(
                "{} 不是写入事件文件",
                path.display()
            )));
        };
        if payload_seq != seq {
            return Err(OpenError::Corrupt(format!(
                "{} 内的序号 {payload_seq} 与文件名序号 {seq} 不一致",
                path.display()
            )));
        }
        // 通过 HTTP 接口不可能产生空批次或同批次重复键；出现即属被篡改/损坏。
        if records.is_empty() {
            return Err(OpenError::Corrupt(format!(
                "{} 是空批次事件，协议不允许",
                path.display()
            )));
        }
        let mut seen = std::collections::HashSet::with_capacity(records.len());
        for record in &records {
            validate_record(record, &path)?;
            if !seen.insert(&record.key) {
                return Err(OpenError::Corrupt(format!(
                    "{} 内出现同批次重复键 {}",
                    path.display(),
                    record.key
                )));
            }
        }
        events.insert(seq, records);
    }
    Ok(events)
}

fn load_snapshots(dir: &Path) -> Result<BTreeMap<u64, (u64, Vec<DiskRecord>)>, OpenError> {
    let mut events = BTreeMap::new();
    for (version, path) in numbered_files(dir)? {
        let EventPayload::Snapshot {
            version: payload_version,
            last_seq,
            records,
        } = read_event(&path)?
        else {
            return Err(OpenError::Corrupt(format!(
                "{} 不是快照文件",
                path.display()
            )));
        };
        if payload_version != version {
            return Err(OpenError::Corrupt(format!(
                "{} 内的版本号 {payload_version} 与文件名版本号 {version} 不一致",
                path.display()
            )));
        }
        for record in &records {
            validate_record(record, &path)?;
        }
        events.insert(version, (last_seq, records));
    }
    Ok(events)
}

fn validate_record(record: &DiskRecord, path: &Path) -> Result<(), OpenError> {
    if !matches!(record.fields, Value::Object(_)) {
        return Err(OpenError::Corrupt(format!(
            "{} 中键 {} 的 fields 不是 JSON 对象",
            path.display(),
            record.key
        )));
    }
    Ok(())
}

/// 读取并校验一个事件文件：头部长度、CRC32、JSON 解析全部通过才算有效。
fn read_event(path: &Path) -> Result<EventPayload, OpenError> {
    let bytes = fs::read(path).map_err(io_err(format!("读取文件 {}", path.display())))?;
    let line_end = bytes
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| OpenError::Corrupt(format!("{} 缺少文件头", path.display())))?;

    let header = std::str::from_utf8(&bytes[..line_end])
        .map_err(|_| OpenError::Corrupt(format!("{} 文件头不是合法 UTF-8", path.display())))?;
    let mut parts = header.split(' ');
    let magic_ok = parts.next() == Some("VDE1");
    let length = parts
        .next()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| OpenError::Corrupt(format!("{} 文件头长度字段非法", path.display())))?;
    let checksum = parts
        .next()
        .and_then(|s| u32::from_str_radix(s, 16).ok())
        .ok_or_else(|| OpenError::Corrupt(format!("{} 文件头校验和字段非法", path.display())))?;
    if !magic_ok || parts.next().is_some() {
        return Err(OpenError::Corrupt(format!(
            "{} 文件头无法识别",
            path.display()
        )));
    }

    let payload = &bytes[line_end + 1..];
    if payload.len() != length {
        return Err(OpenError::Corrupt(format!(
            "{} 声明长度 {length} 但实际为 {}",
            path.display(),
            payload.len()
        )));
    }
    if crc32(payload) != checksum {
        return Err(OpenError::Corrupt(format!(
            "{} CRC32 校验失败，文件可能已损坏或被截断",
            path.display()
        )));
    }

    serde_json::from_slice(payload)
        .map_err(|e| OpenError::Corrupt(format!("{} 内容无法解析: {e}", path.display())))
}

/// 按事件顺序重放，严格验证快照与数据集始终一致，最终构建内存状态。
fn replay(
    writes: BTreeMap<u64, Vec<DiskRecord>>,
    snapshots: BTreeMap<u64, (u64, Vec<DiskRecord>)>,
) -> Result<(Dataset, Vec<Snapshot>, u64), OpenError> {
    let max_seq = writes.keys().next_back().copied().unwrap_or(0);
    let max_version = snapshots.keys().next_back().copied().unwrap_or(0);

    // 序号必须严格连续：文件编号来自崩溃安全的原子提交，出现空档只可能是损坏/缺失。
    for seq in 1..=max_seq {
        if !writes.contains_key(&seq) {
            return Err(OpenError::Corrupt(format!(
                "写入事件 {seq} 缺失，事件序列不连续（最后序号 {max_seq}）"
            )));
        }
    }
    for version in 1..=max_version {
        if !snapshots.contains_key(&version) {
            return Err(OpenError::Corrupt(format!(
                "快照版本 {version} 缺失，版本序列不连续（最后版本 {max_version}）"
            )));
        }
    }

    let mut current: BTreeMap<String, Value> = BTreeMap::new();
    let mut next_seq: u64 = 1;
    let mut previous_last_seq: u64 = 0;
    let mut recovered = Vec::with_capacity(max_version as usize);

    for version in 1..=max_version {
        let (last_seq, records) = &snapshots[&version];

        if *last_seq < previous_last_seq || *last_seq > max_seq {
            return Err(OpenError::Corrupt(format!(
                "快照版本 {version} 的 last_seq={last_seq} 越界或回退"
            )));
        }

        // 重放到该快照保存时刻。
        while next_seq <= *last_seq {
            for record in &writes[&next_seq] {
                current.insert(record.key.clone(), record.fields.clone());
            }
            next_seq += 1;
        }

        // 快照必须与重放结果完全一致，且自身无重复键；否则数据集与快照不一致。
        let mut snapshot_records = BTreeMap::new();
        for record in records {
            if snapshot_records
                .insert(record.key.clone(), record.fields.clone())
                .is_some()
            {
                return Err(OpenError::Corrupt(format!(
                    "快照版本 {version} 中存在重复键 {}",
                    record.key
                )));
            }
        }
        if snapshot_records != current {
            return Err(OpenError::Corrupt(format!(
                "快照版本 {version} 的内容与事件重放结果不一致"
            )));
        }

        recovered.push(Snapshot {
            version,
            records: snapshot_records,
        });
        previous_last_seq = *last_seq;
    }

    // 最新快照之后的写入事件重放到「数据集当前内容」。
    while next_seq <= max_seq {
        for record in &writes[&next_seq] {
            if let Value::Object(fields) = &record.fields {
                current.insert(record.key.clone(), Value::Object(fields.clone()));
            }
        }
        next_seq += 1;
    }

    let dataset: Dataset = current
        .into_iter()
        .map(|(key, value)| match value {
            Value::Object(fields) => (key, fields),
            _ => unreachable!("fields 在 validate_record 已校验为对象"),
        })
        .collect();

    Ok((dataset, recovered, max_seq))
}

// ---------------------------------------------------------------------------
// 运行期提交
// ---------------------------------------------------------------------------

impl Persist {
    /// 原子提交一个成功写入的批次。调用方必须保证只在整批校验通过后调用，
    /// 且磁盘提交成功后才修改内存状态（见 main.rs 中的处理函数）。
    pub(crate) fn commit_writes(
        &mut self,
        batch: &[(String, Map<String, Value>)],
    ) -> io::Result<()> {
        let seq = self.next_seq;
        let payload = EventPayload::Write {
            seq,
            records: batch
                .iter()
                .map(|(key, fields)| DiskRecord {
                    key: key.clone(),
                    fields: Value::Object(fields.clone()),
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&payload).map_err(io::Error::other)?;
        atomic_put(
            &self.root.join("writes"),
            &format!("{seq:020}.json"),
            &mut self.tmp_counter,
            &bytes,
        )?;
        self.next_seq += 1;
        Ok(())
    }

    /// 原子提交一次版本保存。完整快照以最终文件的原子 rename 为提交点。
    pub(crate) fn commit_snapshot(
        &mut self,
        version: u64,
        records: &BTreeMap<String, Value>,
    ) -> io::Result<()> {
        let last_seq = self.next_seq - 1;
        let payload = EventPayload::Snapshot {
            version,
            last_seq,
            records: records
                .iter()
                .map(|(key, fields)| DiskRecord {
                    key: key.clone(),
                    fields: fields.clone(),
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&payload).map_err(io::Error::other)?;
        atomic_put(
            &self.root.join("snapshots"),
            &format!("{version:020}.json"),
            &mut self.tmp_counter,
            &bytes,
        )
    }
}

/// 崩溃安全的「一次性完整落盘」：临时文件写尽并 fsync，再原子 rename 到最终
/// 路径，最后 fsync 目录保证 rename 结果落盘。失败时尽力清理临时文件。
fn atomic_put(dir: &Path, final_name: &str, counter: &mut u64, payload: &[u8]) -> io::Result<()> {
    *counter += 1;
    let tmp_name = format!(".tmp.{}.{}", std::process::id(), *counter);
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(final_name);

    let outcome = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        let header = format!("VDE1 {} {:08x}\n", payload.len(), crc32(payload));
        file.write_all(header.as_bytes())?;
        file.write_all(payload)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(dir)?;
        Ok(())
    })();

    if outcome.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    outcome
}

/// fsync 目录，确保 rename 等目录项变更落盘（std 在 macOS 上使用 F_FULLFSYNC）。
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

// ---------------------------------------------------------------------------
// CRC32 (IEEE 802.3)，用于检测损坏/截断/撕裂的文件内容
// ---------------------------------------------------------------------------

fn crc_table() -> &'static [u32; 256] {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 != 0 {
                    0xEDB8_8320 ^ (crc >> 1)
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    })
}

fn crc32(data: &[u8]) -> u32 {
    let table = crc_table();
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        let index = ((crc ^ *byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[index];
    }
    crc ^ 0xFFFF_FFFF
}
