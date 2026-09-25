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
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

/// 一条记录的字段集：任意 JSON 对象。
type Fields = Map<String, Value>;
/// 当前数据集：BTreeMap 保证列出记录时按键字典序排列。
type Dataset = BTreeMap<String, Fields>;

/// 一份不可变快照：保存时刻数据集的完整深拷贝。
#[derive(Clone)]
struct Snapshot {
    version: u64,
    records: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Store {
    dataset: Dataset,
    /// 已保存快照，按保存顺序排列。
    snapshots: Vec<Snapshot>,
}

impl Store {
    /// 下一个版本号：已保存（含恢复）最大版本号加 1。
    fn next_version(&self) -> u64 {
        self.snapshots
            .iter()
            .map(|s| s.version)
            .max()
            .unwrap_or(0)
            + 1
    }
}

/// 追加式持久化日志：每次成功改变持久状态的操作追加一行 JSON 对象。
///
/// - 批次写入：每条记录一行 `{"type":"write","key":...,"fields":...}`
/// - 保存快照：一行 `{"type":"snapshot","version":...,"total":...}`
///
/// 日志只追加，不改写历史行；启动时按行序重放恢复数据集与全部快照。
struct Journal {
    path: PathBuf,
}

impl Journal {
    fn new(dir: &Path) -> Self {
        Journal {
            path: dir.join("journal.log"),
        }
    }

    /// 把若干行日志原子地追加到日志文件。
    ///
    /// 先把整批行序列化到内存缓冲区，再一次写入；若写入失败，
    /// 把文件截断回写入前的长度，保证不留下半条记录。
    fn append(&self, lines: &[Value]) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut buf = Vec::new();
        for line in lines {
            serde_json::to_writer(&mut buf, line)?;
            buf.push(b'\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let original_len = file.metadata()?.len();
        if let Err(err) = file.write_all(&buf).and_then(|()| file.flush()) {
            // 不留半条记录：截断回写入前的长度。
            let _ = file.set_len(original_len);
            return Err(err);
        }
        Ok(())
    }
}

/// 启动时按日志顺序重放，恢复数据集与全部快照。
///
/// 返回恢复出的 Store；若某行结构非法，则丢弃自该行起的全部内容，
/// 保留此前已重放的状态，并返回该行的行号（从 1 计）由调用方报告。
/// 日志文件或目录不存在等价于空数据集、零快照。
fn replay_journal(dir: &Path) -> (Store, Option<usize>) {
    let path = dir.join("journal.log");
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(_) => return (Store::default(), None),
    };

    let mut store = Store::default();
    for (index, line) in content.lines().enumerate() {
        let line_no = index + 1;
        match parse_journal_line(line) {
            Some(JournalEntry::Write { key, fields }) => {
                store.dataset.insert(key, fields);
            }
            Some(JournalEntry::Snapshot { version }) => {
                // 重放到快照行时，当前数据集即保存时刻的完整状态。
                let records = store
                    .dataset
                    .iter()
                    .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
                    .collect();
                store.snapshots.push(Snapshot { version, records });
            }
            None => return (store, Some(line_no)),
        }
    }
    (store, None)
}

enum JournalEntry {
    Write { key: String, fields: Fields },
    Snapshot { version: u64 },
}

/// 解析一行日志；结构非法（非 JSON 对象、type 非法、缺必需字段或字段类型不符）返回 None。
fn parse_journal_line(line: &str) -> Option<JournalEntry> {
    let Value::Object(object) = serde_json::from_str::<Value>(line).ok()? else {
        return None;
    };
    match object.get("type")?.as_str()? {
        "write" => {
            let key = object.get("key")?.as_str()?.to_owned();
            let Value::Object(fields) = object.get("fields")?.clone() else {
                return None;
            };
            Some(JournalEntry::Write { key, fields })
        }
        "snapshot" => {
            let version = object.get("version")?.as_u64()?;
            object.get("total")?.as_u64()?;
            Some(JournalEntry::Snapshot { version })
        }
        _ => None,
    }
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: Arc<Mutex<Store>>,
    journal: Arc<Journal>,
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

/// 持久化失败：500，响应体 error 说明原因，内存状态与日志均不改变。
fn persist_failed(err: std::io::Error) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: format!("持久化日志写入失败: {err}"),
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
    // 解析与全部校验都在锁外完成；只有整批合法时才持锁一次性提交。
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

    // 全部合法：先落盘后改内存。日志写入失败则整批不生效，数据集保持原状。
    let lines: Vec<Value> = batch
        .iter()
        .map(|(key, fields)| json!({"type": "write", "key": key, "fields": fields}))
        .collect();

    let mut store = state.store.lock().unwrap();
    state.journal.append(&lines).map_err(persist_failed)?;
    let written = batch.len();
    for (key, fields) in batch {
        store.dataset.insert(key, fields);
    }
    Ok(Json(WriteResponse {
        written,
        total: store.dataset.len(),
    }))
}

/// POST /versions：把当前数据集固化为不可变快照。
async fn save_version(
    State(state): State<AppState>,
) -> Result<Json<SaveResponse>, (StatusCode, Json<ErrorBody>)> {
    let mut store: MutexGuard<Store> = state.store.lock().unwrap();
    let version = store.next_version();
    let total = store.dataset.len();
    // 先落盘：日志写入失败则不产生新快照。
    state
        .journal
        .append(&[json!({"type": "snapshot", "version": version, "total": total})])
        .map_err(persist_failed)?;
    let records = store
        .dataset
        .iter()
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    store.snapshots.push(Snapshot { version, records });
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
    // 持久化目录：VDE_DATA_DIR，默认 ./data；日志文件为其下的 journal.log。
    let data_dir = PathBuf::from(env::var("VDE_DATA_DIR").unwrap_or_else(|_| "./data".to_owned()));

    let (store, discarded_from) = replay_journal(&data_dir);
    if let Some(line_no) = discarded_from {
        println!("持久化日志自第 {line_no} 行起结构非法，已丢弃该行及后续全部内容");
    }

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        journal: Arc::new(Journal::new(&data_dir)),
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
