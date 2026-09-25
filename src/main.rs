use axum::{
    Json, Router,
    body::Bytes,
    extract::{Request, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
    routing::{get, post},
};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{Mutex, MutexGuard},
};

/// 一条记录的字段集：任意 JSON 对象。
type Fields = Map<String, Value>;
/// 当前数据集：BTreeMap 保证列出记录时按键字典序排列。
type Dataset = BTreeMap<String, Fields>;

/// 持久化日志文件名（位于 VDE_DATA_DIR 给定的数据目录下）。
const JOURNAL_FILE: &str = "journal.log";

/// 一份不可变快照：保存时刻数据集的完整深拷贝。
#[derive(Clone)]
struct Snapshot {
    version: u64,
    records: BTreeMap<String, Value>,
}

struct Store {
    dataset: Dataset,
    /// 已保存快照，按下标顺序保存；版本号由 Snapshot::version 自带。
    snapshots: Vec<Snapshot>,
    /// 下一个快照使用的版本号；空数据集时为 1，恢复后在最大版本号上递增。
    next_version: u64,
    /// 只追加的持久化日志句柄；与 dataset/snapshots 同处一把锁内串行访问。
    journal: fs::File,
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入、落盘与快照保存对并发交错整体可见。
    store: std::sync::Arc<Mutex<Store>>,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
}

/// 写入请求：一批记录。用 Value 接收以便逐条校验并报告出错下标。
#[derive(Deserialize)]
struct WriteRequest {
    records: Vec<Value>,
}

#[derive(Serialize)]
struct RecordOut {
    key: String,
    fields: Value,
}

#[derive(Serialize)]
struct WriteResponse {
    written: usize,
    total: usize,
}

#[derive(Serialize)]
struct SaveResponse {
    version: u64,
    total: usize,
}

#[derive(Serialize)]
struct SnapshotSummary {
    version: u64,
    records: Vec<RecordOut>,
}

#[derive(Serialize)]
struct SnapshotRecord {
    version: u64,
    key: String,
    fields: Value,
}

/// 日志中的一条批次写入记录。
#[derive(Serialize)]
struct JournalWrite<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    key: &'a str,
    fields: &'a Fields,
}

/// 日志中的一条快照保存记录。
#[derive(Serialize)]
struct JournalSnapshot {
    #[serde(rename = "type")]
    kind: &'static str,
    version: u64,
    total: usize,
}

/// GET /snapshots 查询参数：必选 version，可选 key。
struct SnapshotParams {
    version: u64,
    key: Option<String>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
}

fn bad_request(message: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorBody {
            error: message.into(),
            index: None,
        }),
    )
}

fn server_error(message: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: message.into(),
            index: None,
        }),
    )
}

/// 手动解析查询串（percent-decode 后取 version / key）。
fn parse_snapshot_query(uri: &Uri) -> Result<SnapshotParams, String> {
    let mut version: Option<u64> = None;
    let mut key: Option<String> = None;
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            let Some((raw_name, raw_value)) = pair.split_once('=') else {
                return Err(format!("非法查询参数: {pair}"));
            };
            let name = percent_decode_str(raw_name).decode_utf8_lossy();
            let value = percent_decode_str(raw_value).decode_utf8_lossy();
            match name.as_ref() {
                "version" => {
                    version = Some(
                        value
                            .parse()
                            .map_err(|_| format!("version 必须是正整数: {value}"))?,
                    );
                }
                "key" => key = Some(value.into_owned()),
                other => return Err(format!("未知查询参数: {other}")),
            }
        }
    }
    match version {
        Some(version) => Ok(SnapshotParams { version, key }),
        None => Err("缺少必填查询参数 version".to_owned()),
    }
}

/// 把当前数据集深拷贝为快照保存格式（fields 包回 JSON 对象）。
fn snapshot_dataset(dataset: &Dataset) -> BTreeMap<String, Value> {
    dataset
        .iter()
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect()
}

impl Store {
    /// 准备数据目录、按日志顺序重放恢复状态，并打开日志供追加。
    ///
    /// 日志中自第一个非法行起的全部内容会被丢弃（文件截断到合法前缀），
    /// 同时在 stdout 打印被丢弃的起始行号（从 1 计）。目录不存在或日志为空
    /// 等价于空数据集、零快照。
    fn load(data_dir: &str) -> io::Result<Store> {
        fs::create_dir_all(data_dir)?;
        let journal_path = Path::new(data_dir).join(JOURNAL_FILE);

        let mut dataset: Dataset = Dataset::new();
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut next_version: u64 = 1;
        let mut truncate_at: Option<u64> = None;
        let mut ends_with_newline = true;

        if journal_path.exists() {
            let data = fs::read(&journal_path)?;
            ends_with_newline = data.is_empty() || data.last() == Some(&b'\n');
            // 去掉最后一个换行后按行切分；末尾换行不产生空行。
            let content = match data.last() {
                Some(b'\n') => &data[..data.len() - 1],
                _ => data.as_slice(),
            };
            if !content.is_empty() {
                // pos 为当前行行首相对文件头的字节偏移，也是丢弃该行时的截断点。
                let mut pos: u64 = 0;
                for (index, line) in content.split(|b| *b == b'\n').enumerate() {
                    let line_no = index + 1;
                    match replay_line(line, &mut dataset, &mut snapshots, &mut next_version) {
                        Ok(()) => pos += line.len() as u64 + 1,
                        Err(()) => {
                            println!(
                                "日志 {} 第 {line_no} 行结构非法，已丢弃自该行起的全部内容",
                                journal_path.display()
                            );
                            truncate_at = Some(pos);
                            break;
                        }
                    }
                }
            }
        }

        if let Some(len) = truncate_at {
            // 丢弃非法尾部：历史合法行原样保留，文件截到第一个非法行行首。
            fs::OpenOptions::new()
                .write(true)
                .open(&journal_path)?
                .set_len(len)?;
        }

        let mut journal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)?;

        // 合法前缀若缺少结尾换行（例如上次崩溃留下的完整末行），先补上换行，
        // 否则下一次追加行会与末行粘连成一行。
        if truncate_at.is_none() && !ends_with_newline {
            journal.write_all(b"\n")?;
            journal.sync_data()?;
        }

        Ok(Store {
            dataset,
            snapshots,
            next_version,
            journal,
        })
    }

    /// 把一批已校验的写入整体追加到日志并落盘；成功后才提交进内存。
    ///
    /// 任一日志 I/O 失败：尽力把文件截回本批次之前的长度，内存状态保持不变，
    /// 日志中不会留下本批次的半条记录。
    fn commit_writes(&mut self, batch: &[(String, Fields)]) -> io::Result<(usize, usize)> {
        let start_len = self.journal.metadata()?.len();

        let mut buf: Vec<u8> = Vec::new();
        for (key, fields) in batch {
            let entry = JournalWrite {
                kind: "write",
                key,
                fields,
            };
            serde_json::to_writer(&mut buf, &entry).expect("journal entry serializes");
            buf.push(b'\n');
        }

        if let Err(error) = self
            .journal
            .write_all(&buf)
            .and_then(|_| self.journal.sync_data())
        {
            self.rollback_journal(start_len);
            return Err(error);
        }

        for (key, fields) in batch {
            self.dataset.insert(key.clone(), fields.clone());
        }
        Ok((batch.len(), self.dataset.len()))
    }

    /// 把快照保存记录追加到日志并落盘；成功后才在内存中保存快照。
    fn commit_snapshot(&mut self) -> io::Result<(u64, usize)> {
        let version = self.next_version;
        let total = self.dataset.len();
        let start_len = self.journal.metadata()?.len();

        let mut buf: Vec<u8> = Vec::new();
        serde_json::to_writer(
            &mut buf,
            &JournalSnapshot {
                kind: "snapshot",
                version,
                total,
            },
        )
        .expect("journal entry serializes");
        buf.push(b'\n');

        if let Err(error) = self
            .journal
            .write_all(&buf)
            .and_then(|_| self.journal.sync_data())
        {
            self.rollback_journal(start_len);
            return Err(error);
        }

        self.snapshots.push(Snapshot {
            version,
            records: snapshot_dataset(&self.dataset),
        });
        self.next_version += 1;
        Ok((version, total))
    }

    /// 日志写入失败后的尽力回滚：截回操作前长度，丢弃可能部分写入的字节。
    fn rollback_journal(&mut self, start_len: u64) {
        if self.journal.set_len(start_len).is_ok() {
            let _ = self.journal.sync_data();
        }
    }
}

/// 重放一行日志，就地应用到数据集/快照集合。行结构非法时返回 Err。
fn replay_line(
    line: &[u8],
    dataset: &mut Dataset,
    snapshots: &mut Vec<Snapshot>,
    next_version: &mut u64,
) -> Result<(), ()> {
    let value: Value = serde_json::from_slice(line).map_err(|_| ())?;
    let object = value.as_object().ok_or(())?;

    match object.get("type").and_then(Value::as_str) {
        Some("write") => {
            let key = object.get("key").and_then(Value::as_str).ok_or(())?;
            let fields = object
                .get("fields")
                .and_then(Value::as_object)
                .ok_or(())?
                .clone();
            dataset.insert(key.to_owned(), fields);
        }
        Some("snapshot") => {
            let version = object
                .get("version")
                .and_then(Value::as_u64)
                .filter(|v| *v >= 1)
                .ok_or(())?;
            // total 为必需字段且必须是非负整数；快照内容取保存时刻的数据集。
            if object.get("total").and_then(Value::as_u64).is_none() {
                return Err(());
            }
            snapshots.push(Snapshot {
                version,
                records: snapshot_dataset(dataset),
            });
            *next_version = (*next_version).max(version + 1);
        }
        _ => return Err(()),
    }
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn version() -> Json<Version> {
    Json(Version {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// POST /datasets/records：整体成功或整体失败的批量写入。
async fn write_records(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<WriteResponse>, (StatusCode, Json<ErrorBody>)> {
    // 解析与全部校验都在锁外完成；只有整批合法时才持锁一次性提交并落盘。
    let parsed: WriteRequest = serde_json::from_slice(&body)
        .map_err(|e| bad_request(format!("请求必须是包含 records 数组的 JSON 对象: {e}")))?;

    if parsed.records.is_empty() {
        return Err(bad_request("records 不能为空批次"));
    }

    let mut seen = HashSet::with_capacity(parsed.records.len());
    let mut batch: Vec<(String, Fields)> = Vec::with_capacity(parsed.records.len());

    for (index, record) in parsed.records.into_iter().enumerate() {
        let fail = |message: &'static str| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: message.to_owned(),
                    index: Some(index),
                }),
            )
        };

        let mut object = match record {
            Value::Object(map) => map,
            _ => return Err(fail("每条记录必须是 JSON 对象")),
        };

        let key = match object.remove("key") {
            Some(Value::String(key)) => key,
            Some(_) => return Err(fail("记录的 key 必须是字符串")),
            None => return Err(fail("记录缺少键 key")),
        };

        let fields = match object.remove("fields") {
            Some(Value::Object(fields)) => fields,
            Some(_) => return Err(fail("记录的 fields 必须是 JSON 对象")),
            None => return Err(fail("记录缺少字段集 fields")),
        };

        if !seen.insert(key.clone()) {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("批次内出现重复键: {key}"),
                    index: Some(index),
                }),
            ));
        }

        batch.push((key, fields));
    }

    // 全部合法：持锁整体追加日志并提交。落盘失败则整批回滚，返回 500。
    let mut store = state.store.lock().unwrap();
    let (written, total) = store
        .commit_writes(&batch)
        .map_err(|e| server_error(format!("持久化日志写入失败: {e}")))?;
    Ok(Json(WriteResponse { written, total }))
}

/// POST /versions：把当前数据集固化为不可变快照。
async fn save_version(
    State(state): State<AppState>,
) -> Result<Json<SaveResponse>, (StatusCode, Json<ErrorBody>)> {
    let mut store: MutexGuard<Store> = state.store.lock().unwrap();
    let (version, total) = store
        .commit_snapshot()
        .map_err(|e| server_error(format!("持久化日志写入失败: {e}")))?;
    Ok(Json(SaveResponse { version, total }))
}

/// GET /snapshots?version=N[&key=K]：读取历史快照。
async fn get_snapshot(
    State(state): State<AppState>,
    req: Request,
) -> Result<axum::response::Response, (StatusCode, Json<ErrorBody>)> {
    let params = parse_snapshot_query(req.uri()).map_err(bad_request)?;

    let store = state.store.lock().unwrap();
    let snapshot = store
        .snapshots
        .iter()
        .find(|s| s.version == params.version)
        .cloned()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("版本 {} 不存在或尚未保存", params.version),
                    index: None,
                }),
            )
        })?;

    match params.key {
        Some(key) => {
            let fields = snapshot.records.get(&key).cloned().ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("版本 {} 中不存在键 {key}", snapshot.version),
                        index: None,
                    }),
                )
            })?;
            Ok(Json(SnapshotRecord {
                version: snapshot.version,
                key,
                fields,
            })
            .into_response())
        }
        None => {
            // BTreeMap 的迭代顺序即键的字典序。
            let records = snapshot
                .records
                .into_iter()
                .map(|(key, fields)| RecordOut { key, fields })
                .collect();
            Ok(Json(SnapshotSummary {
                version: snapshot.version,
                records,
            })
            .into_response())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let data_dir = env::var("VDE_DATA_DIR").unwrap_or_else(|_| "./data".to_owned());

    // 启动重放：目录或日志缺失等价于空数据集；损坏尾部会被截断并提示行号。
    let store = Store::load(&data_dir)?;

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(store)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/snapshots", get(get_snapshot))
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    /// 为每个测试准备独立的临时数据目录（进程内串行测试，名称固定即可）。
    fn test_dir(name: &str) -> PathBuf {
        let dir = env::temp_dir().join(format!("vde-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn record(key: &str, fields: Value) -> (String, Fields) {
        (key.to_owned(), fields.as_object().unwrap().clone())
    }

    #[test]
    fn replay_rebuilds_dataset_snapshots_and_version() {
        let dir = test_dir("replay");
        let path = dir.join(JOURNAL_FILE);
        fs::write(
            &path,
            concat!(
                "{\"type\":\"write\",\"key\":\"a\",\"fields\":{\"n\":1}}\n",
                "{\"type\":\"write\",\"key\":\"b\",\"fields\":{\"n\":2}}\n",
                "{\"type\":\"snapshot\",\"version\":1,\"total\":2}\n",
                "{\"type\":\"write\",\"key\":\"a\",\"fields\":{\"n\":11}}\n",
                "{\"type\":\"snapshot\",\"version\":2,\"total\":2}\n",
            ),
        )
        .unwrap();

        let store = Store::load(dir.to_str().unwrap()).unwrap();
        assert_eq!(store.dataset.len(), 2);
        assert_eq!(store.dataset["a"]["n"], 11);
        assert_eq!(store.snapshots.len(), 2);
        // 快照 1 保存的是覆盖前的数据集。
        assert_eq!(store.snapshots[0].records["a"]["n"], 1);
        assert_eq!(store.snapshots[1].records["a"]["n"], 11);
        assert_eq!(store.next_version, 3);
    }

    #[test]
    fn empty_or_missing_dir_means_empty_state() {
        let dir = test_dir("empty-dir");
        let store = Store::load(dir.to_str().unwrap()).unwrap();
        assert!(store.dataset.is_empty());
        assert!(store.snapshots.is_empty());
        assert_eq!(store.next_version, 1);

        fs::write(dir.join(JOURNAL_FILE), b"").unwrap();
        let store = Store::load(dir.to_str().unwrap()).unwrap();
        assert_eq!(store.next_version, 1);
    }

    #[test]
    fn corrupt_line_drops_tail_and_keeps_replayed_prefix() {
        let dir = test_dir("corrupt");
        let path = dir.join(JOURNAL_FILE);
        fs::write(
            &path,
            concat!(
                "{\"type\":\"write\",\"key\":\"a\",\"fields\":{\"n\":1}}\n",
                "{\"type\":\"snapshot\",\"version\":1,\"total\":1}\n",
                "{\"type\":\"bogus\"}\n",
                "{\"type\":\"write\",\"key\":\"b\",\"fields\":{\"n\":2}}\n",
            ),
        )
        .unwrap();

        let store = Store::load(dir.to_str().unwrap()).unwrap();
        assert_eq!(store.dataset.len(), 1);
        assert!(store.dataset.contains_key("a"));
        assert_eq!(store.snapshots.len(), 1);
        assert_eq!(store.next_version, 2);

        // 文件被截到第 3 行行首：只剩前两行。
        let remaining = fs::read_to_string(&path).unwrap();
        assert_eq!(remaining.matches('\n').count(), 2);
        assert!(!remaining.contains("bogus"));
        assert!(!remaining.contains("\"b\""));
    }

    #[test]
    fn rejects_each_kind_of_malformed_line() {
        for bad in [
            "not json",
            "[]",
            "\"string\"",
            "42",
            "{\"type\":\"write\",\"key\":\"a\"}",
            "{\"type\":\"write\",\"fields\":{\"n\":1}}",
            "{\"type\":\"write\",\"key\":1,\"fields\":{}}",
            "{\"type\":\"snapshot\",\"version\":1}",
            "{\"type\":\"snapshot\",\"version\":0,\"total\":0}",
            "{\"type\":\"snapshot\",\"total\":1}",
            "{\"type\":\"other\",\"k\":1}",
        ] {
            let dir = test_dir("malformed");
            fs::write(
                dir.join(JOURNAL_FILE),
                format!("{{\"type\":\"write\",\"key\":\"a\",\"fields\":{{}}}}\n{bad}\n"),
            )
            .unwrap();
            let store = Store::load(dir.to_str().unwrap()).unwrap();
            assert_eq!(store.dataset.len(), 1, "line should be rejected: {bad}");
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn valid_last_line_without_newline_gets_completed() {
        let dir = test_dir("no-final-newline");
        let path = dir.join(JOURNAL_FILE);
        fs::write(
            &path,
            "{\"type\":\"write\",\"key\":\"a\",\"fields\":{\"n\":1}}",
        )
        .unwrap();

        let mut store = Store::load(dir.to_str().unwrap()).unwrap();
        assert_eq!(store.dataset.len(), 1);
        let raw = fs::read(&path).unwrap();
        assert_eq!(raw.last(), Some(&b'\n'));

        // 补换行后后续追加不与旧行粘连。
        let (written, total) = store
            .commit_writes(&[record("b", json!({"n": 2}))])
            .unwrap();
        assert_eq!((written, total), (1, 2));
        let raw_after = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = raw_after.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn failed_batch_write_changes_nothing() {
        let dir = test_dir("write-iofail");
        let path = dir.join(JOURNAL_FILE);
        let mut store = Store::load(dir.to_str().unwrap()).unwrap();
        store
            .commit_writes(&[record("a", json!({"n": 1}))])
            .unwrap();

        let before = fs::read(&path).unwrap();
        // 换成只读 fd：任何 write 立即失败，且不会写入半个字节。
        store.journal = fs::File::open(&path).unwrap();
        let err = store
            .commit_writes(&[record("b", json!({"n": 2}))])
            .unwrap_err();
        assert!(err.to_string().contains("denied") || err.raw_os_error().is_some());

        // 内存数据集不变、版本号不变、磁盘文件一个字节不多。
        assert_eq!(store.dataset.len(), 1);
        assert!(store.dataset.contains_key("a"));
        assert_eq!(store.next_version, 1);
        assert_eq!(fs::read(&path).unwrap(), before);

        // 恢复可写句柄后整批可以正常提交。
        store.journal = OpenOptions::new().append(true).open(&path).unwrap();
        let (written, total) = store
            .commit_writes(&[record("b", json!({"n": 2}))])
            .unwrap();
        assert_eq!((written, total), (1, 2));
    }

    #[test]
    fn failed_snapshot_save_changes_nothing() {
        let dir = test_dir("snapshot-iofail");
        let path = dir.join(JOURNAL_FILE);
        let mut store = Store::load(dir.to_str().unwrap()).unwrap();
        store
            .commit_writes(&[record("a", json!({"n": 1}))])
            .unwrap();

        let before = fs::read(&path).unwrap();
        store.journal = fs::File::open(&path).unwrap();
        assert!(store.commit_snapshot().is_err());

        // 快照不保存、版本号不消耗。
        assert!(store.snapshots.is_empty());
        assert_eq!(store.next_version, 1);
        assert_eq!(fs::read(&path).unwrap(), before);

        // 恢复后下一次保存拿到的仍是版本 1。
        store.journal = OpenOptions::new().append(true).open(&path).unwrap();
        let (version, total) = store.commit_snapshot().unwrap();
        assert_eq!((version, total), (1, 1));
        assert_eq!(store.snapshots.len(), 1);
        assert_eq!(store.next_version, 2);
    }
}

