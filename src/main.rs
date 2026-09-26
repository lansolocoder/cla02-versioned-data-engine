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
    path::{Path, PathBuf},
    process,
    sync::{Mutex, MutexGuard},
};

/// 一条记录的字段集：任意 JSON 对象。
type Fields = Map<String, Value>;
/// 当前数据集：BTreeMap 保证列出记录时按键字典序排列。
type Dataset = BTreeMap<String, Fields>;

/// 一份不可变快照：保存时刻数据集的完整深拷贝。
#[derive(Clone)]
struct Snapshot {
    version: u64,
    records: BTreeMap<String, Fields>,
}

#[derive(Default)]
struct Store {
    dataset: Dataset,
    /// 已保存快照，下标 0 对应版本 1。
    snapshots: Vec<Snapshot>,
}

/// 数据文件的磁盘格式：当前数据集与全部快照。
/// BTreeMap 序列化为 JSON 对象，Map 序列化为字段对象，
/// 数字、字符串、嵌套结构均由 serde_json::Value 原样往返。
#[derive(Serialize, Deserialize)]
struct PersistedState {
    dataset: BTreeMap<String, Fields>,
    snapshots: Vec<PersistedSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u64,
    records: BTreeMap<String, Fields>,
}

impl PersistedState {
    /// 解析后做整体结构校验：版本号必须从 1 起严格连续。
    /// 校验通过才构造内存 Store，杜绝半份数据对外服务。
    fn into_store(self) -> Result<Store, String> {
        for (index, snapshot) in self.snapshots.iter().enumerate() {
            let expected = index as u64 + 1;
            if snapshot.version != expected {
                return Err(format!(
                    "快照版本号必须从 1 起严格连续递增：下标 {index} 处版本号为 {}，应为 {expected}",
                    snapshot.version
                ));
            }
        }
        Ok(Store {
            dataset: self.dataset,
            snapshots: self
                .snapshots
                .into_iter()
                .map(|s| Snapshot {
                    version: s.version,
                    records: s.records,
                })
                .collect(),
        })
    }
}

impl From<&Store> for PersistedState {
    fn from(store: &Store) -> Self {
        PersistedState {
            dataset: store.dataset.clone(),
            snapshots: store
                .snapshots
                .iter()
                .map(|s| PersistedSnapshot {
                    version: s.version,
                    records: s.records.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: std::sync::Arc<Mutex<Store>>,
    /// 持久化数据文件路径（$VDE_DATA_DIR/state.json）。
    data_file: std::sync::Arc<PathBuf>,
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

    // 全部合法：持锁整体覆盖写入并持久化。任一条非法都在上面提前返回，数据集保持原状。
    let mut store = state.store.lock().unwrap();
    let written = batch.len();
    // 修改前完整备份；持久化失败时整体恢复，保证批次写入要么全成要么全败。
    let previous = store.dataset.clone();
    for (key, fields) in batch {
        store.dataset.insert(key, fields);
    }

    if let Err(message) = persist(&state.data_file, &store) {
        store.dataset = previous;
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("状态持久化失败，已回滚本次写入: {message}"),
                index: None,
            }),
        ));
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
    let version = store.snapshots.len() as u64 + 1;
    let records = store
        .dataset
        .iter()
        .map(|(key, fields)| (key.clone(), fields.clone()))
        .collect();
    let total = store.dataset.len();
    store.snapshots.push(Snapshot { version, records });

    if let Err(message) = persist(&state.data_file, &store) {
        store.snapshots.pop();
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("状态持久化失败，快照未保存: {message}"),
                index: None,
            }),
        ));
    }

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
                fields: Value::Object(fields),
            })
            .into_response())
        }
        None => {
            // BTreeMap 的迭代顺序即键的字典序。
            let records = snapshot
                .records
                .into_iter()
                .map(|(key, fields)| RecordOut {
                    key,
                    fields: Value::Object(fields),
                })
                .collect();
            Ok(Json(SnapshotSummary {
                version: snapshot.version,
                records,
            })
            .into_response())
        }
    }
}

/// 确定持久化目录：环境变量 VDE_DATA_DIR，缺省为当前工作目录下的 data。
fn data_dir() -> PathBuf {
    env::var_os("VDE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"))
}

/// 启动时从数据文件恢复状态。
///
/// - 文件不存在：按空数据集、尚未保存任何快照启动，并确保目录已创建。
/// - 文件存在但无法解析或结构不合法：错误写入 stderr 并以非 0 退出码终止，
///   不修改原文件，也不会载入半份数据后对外服务。
fn load_state(data_file: &Path) -> Store {
    let bytes = match std::fs::read(data_file) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Store::default(),
        Err(err) => {
            eprintln!("无法读取数据文件 {}: {err}", data_file.display());
            process::exit(1);
        }
    };

    let persisted: PersistedState = match serde_json::from_slice(&bytes) {
        Ok(persisted) => persisted,
        Err(err) => {
            eprintln!(
                "数据文件 {} 已损坏（不是合法 JSON 或结构不符合要求），拒绝启动: {err}",
                data_file.display()
            );
            process::exit(1);
        }
    };

    match persisted.into_store() {
        Ok(store) => store,
        Err(message) => {
            eprintln!("数据文件 {} 内容不合法，拒绝启动: {message}", data_file.display());
            process::exit(1);
        }
    }
}

/// 把当前状态原子写入数据文件：先写同目录临时文件并落盘，再 rename 覆盖。
/// 任何时刻数据文件要么是上一份完整状态、要么是新一份完整状态，不会出现半截内容。
fn persist(data_file: &Path, store: &Store) -> std::io::Result<()> {
    let dir = data_file.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;

    let tmp_path = dir.join(format!(
        ".{}.tmp",
        data_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state.json")
    ));

    let payload = serde_json::to_vec(&PersistedState::from(store))
        .map_err(std::io::Error::other)?;

    {
        use std::io::Write;
        let mut tmp = std::fs::File::create(&tmp_path)?;
        tmp.write_all(&payload)?;
        tmp.sync_all()?;
    }
    std::fs::rename(&tmp_path, data_file)?;
    // 尽力确保目录项（rename）落盘；失败不影响文件本身的原子完整性。
    if let Ok(dir_file) = std::fs::File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    let dir = data_dir();
    let data_file = dir.join("state.json");
    // 文件不存在时确保持久化目录存在；文件存在则直接进入恢复。
    if !data_file.exists() {
        std::fs::create_dir_all(&dir)?;
    }
    let store = load_state(&data_file);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(store)),
        data_file: std::sync::Arc::new(data_file),
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
