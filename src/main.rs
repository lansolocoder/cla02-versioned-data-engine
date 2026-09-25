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
    ffi::OsStr,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
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
    records: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Store {
    dataset: Dataset,
    /// 已保存快照，下标 0 对应版本 1。
    snapshots: Vec<Snapshot>,
}

/// 磁盘上的状态文件布局。版本号显式存储，恢复后版本序列严格接续旧序列。
#[derive(Serialize, Deserialize)]
struct PersistedState {
    /// 固定为 1，供未来格式演进时区分。
    format: u32,
    /// 当前数据集，序列化为 [key, fields] 对的数组。
    dataset: Vec<PersistedRecord>,
    /// 已保存快照，版本号必须从 1 起严格连续。
    snapshots: Vec<PersistedSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct PersistedRecord {
    key: String,
    fields: Value,
}

#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u64,
    records: Vec<PersistedRecord>,
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: std::sync::Arc<Mutex<Store>>,
    /// 状态文件路径；每次成功提交后原子重写该文件。
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

fn server_error(message: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: message.into(),
            index: None,
        }),
    )
}

/// 给路径末尾再追加一个扩展名：`vde-state.json` -> `vde-state.json.tmp`。
trait WithExtraExtension {
    fn with_extra_extension(&self, extra: &str) -> PathBuf;
}

impl WithExtraExtension for Path {
    fn with_extra_extension(&self, extra: &str) -> PathBuf {
        let mut file_name = self.file_name().map(OsStr::to_owned).unwrap_or_default();
        file_name.push(format!(".{extra}"));
        self.with_file_name(file_name)
    }
}

/// 状态文件名，位于 VDE_DATA_DIR（缺省为当前工作目录下的 data）。
const STATE_FILE_NAME: &str = "vde-state.json";

fn data_file_path() -> PathBuf {
    let dir = env::var_os("VDE_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));
    dir.join(STATE_FILE_NAME)
}

/// 把内存状态序列化并原子替换状态文件：先写同目录临时文件并 fsync，再 rename。
/// 任何时刻状态文件要么是旧的完整内容、要么是新的完整内容，不会留下截断文件。
fn persist_store(store: &Store, data_file: &Path) -> std::io::Result<()> {
    let persisted = PersistedState {
        format: 1,
        dataset: store
            .dataset
            .iter()
            .map(|(key, fields)| PersistedRecord {
                key: key.clone(),
                fields: Value::Object(fields.clone()),
            })
            .collect(),
        snapshots: store
            .snapshots
            .iter()
            .map(|s| PersistedSnapshot {
                version: s.version,
                records: s
                    .records
                    .iter()
                    .map(|(key, fields)| PersistedRecord {
                        key: key.clone(),
                        fields: fields.clone(),
                    })
                    .collect(),
            })
            .collect(),
    };

    // 序列化在持锁状态下完成；失败不会触及磁盘文件。
    let payload = serde_json::to_vec_pretty(&persisted).map_err(std::io::Error::other)?;

    if let Some(dir) = data_file.parent() {
        fs::create_dir_all(dir)?;
    }

    let tmp = data_file.with_extra_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    fs::rename(&tmp, data_file)?;

    // fsync 目录，确保 rename 本身落盘（崩溃恢复语义更稳）。
    if let Some(dir) = data_file.parent()
        && let Ok(dir_file) = File::open(dir)
    {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

/// 严格校验并恢复状态。文件不存在时返回空状态；文件存在但无法解析或结构不合法时
/// 返回错误，调用方必须拒绝启动，且本函数绝不改动原文件。
fn load_store(data_file: &Path) -> Result<Store, String> {
    let bytes = match fs::read(data_file) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Store::default()),
        Err(e) => return Err(format!("无法读取状态文件 {}: {e}", data_file.display())),
    };

    let persisted: PersistedState = serde_json::from_slice(&bytes).map_err(|e| {
        format!(
            "状态文件 {} 不是合法 JSON，拒绝启动: {e}",
            data_file.display()
        )
    })?;

    if persisted.format != 1 {
        return Err(format!(
            "状态文件 {} 的 format 为 {}，本版本仅支持 1",
            data_file.display(),
            persisted.format
        ));
    }

    let mut dataset: Dataset = BTreeMap::new();
    for record in persisted.dataset {
        let Value::Object(fields) = record.fields else {
            return Err(format!(
                "状态文件 {} 中数据集键 {} 的 fields 不是 JSON 对象",
                data_file.display(),
                record.key
            ));
        };
        if dataset.insert(record.key.clone(), fields).is_some() {
            return Err(format!(
                "状态文件 {} 的数据集中键 {} 重复",
                data_file.display(),
                record.key
            ));
        }
    }

    let mut snapshots: Vec<Snapshot> = Vec::with_capacity(persisted.snapshots.len());
    for (index, snap) in persisted.snapshots.into_iter().enumerate() {
        let expected = index as u64 + 1;
        if snap.version != expected {
            return Err(format!(
                "状态文件 {} 的快照版本序列不连续：第 {} 份快照版本为 {}，应为 {}",
                data_file.display(),
                expected,
                snap.version,
                expected
            ));
        }
        let mut records: BTreeMap<String, Value> = BTreeMap::new();
        for record in snap.records {
            let Value::Object(_) = &record.fields else {
                return Err(format!(
                    "状态文件 {} 中版本 {} 键 {} 的 fields 不是 JSON 对象",
                    data_file.display(),
                    snap.version,
                    record.key
                ));
            };
            if records.insert(record.key.clone(), record.fields).is_some() {
                return Err(format!(
                    "状态文件 {} 的版本 {} 中键 {} 重复",
                    data_file.display(),
                    snap.version,
                    record.key
                ));
            }
        }
        snapshots.push(Snapshot {
            version: snap.version,
            records,
        });
    }

    Ok(Store { dataset, snapshots })
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

    // 全部合法：持锁整体覆盖写入，并在同一临界区把新状态落盘。
    // 落盘失败则精确回滚本批改动、返回 500，数据集保持请求前状态。
    let mut store = state.store.lock().unwrap();
    let mut previous: Vec<(String, Option<Fields>)> = Vec::with_capacity(batch.len());
    for (key, fields) in &batch {
        previous.push((key.clone(), store.dataset.get(key).cloned()));
        store.dataset.insert(key.clone(), fields.clone());
    }
    if let Err(e) = persist_store(&store, &state.data_file) {
        for (key, old) in previous {
            match old {
                Some(fields) => {
                    store.dataset.insert(key, fields);
                }
                None => {
                    store.dataset.remove(&key);
                }
            }
        }
        return Err(server_error(format!("状态落盘失败: {e}")));
    }
    let total = store.dataset.len();
    Ok(Json(WriteResponse {
        written: batch.len(),
        total,
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
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    let total = store.dataset.len();
    store.snapshots.push(Snapshot { version, records });

    // 与写入同一套原子提交：落盘失败则撤回刚追加的快照。
    if let Err(e) = persist_store(&store, &state.data_file) {
        store.snapshots.pop();
        return Err(server_error(format!("状态落盘失败: {e}")));
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
    let data_file = std::sync::Arc::new(data_file_path());

    // 启动恢复必须在对外服务之前完成：文件损坏则报错到 stderr 并非零退出，
    // 不绑定端口、不载入半份数据、也不改动原文件。
    let store = match load_store(&data_file) {
        Ok(store) => store,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    };
    if data_file.exists() {
        eprintln!(
            "已从 {} 恢复：数据集 {} 条记录，快照 {} 个版本",
            data_file.display(),
            store.dataset.len(),
            store.snapshots.len()
        );
    } else {
        eprintln!(
            "状态文件 {} 不存在，以空数据集启动（尚无快照）",
            data_file.display()
        );
    }

    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(store)),
        data_file,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/snapshots", get(get_snapshot))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
