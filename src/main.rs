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
    io::Write,
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
    /// 已保存快照，下标 0 对应版本 1。
    snapshots: Vec<Snapshot>,
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: Arc<Mutex<Store>>,
}

/// 落盘文件格式（内部契约，不对外公开）：当前数据集 + 全部已保存快照。
/// 每份快照冗余记录总数 total，加载时校验版本连续性与记录数一致性。
#[derive(Serialize, Deserialize)]
struct PersistedState {
    dataset: Dataset,
    snapshots: Vec<PersistedSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u64,
    total: usize,
    records: BTreeMap<String, Value>,
}

/// 启动时从 VDE_DATA 恢复状态。文件不存在时按空数据集与零快照启动；
/// 文件存在但格式非法或内容自相矛盾时返回错误，由调用方决定退出。
fn load_store(path: &str) -> Result<Store, String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Store::default()),
        Err(err) => return Err(format!("读取失败: {err}")),
    };

    let persisted: PersistedState =
        serde_json::from_slice(&bytes).map_err(|e| format!("文件格式非法: {e}"))?;

    let mut snapshots = Vec::with_capacity(persisted.snapshots.len());
    for (index, snap) in persisted.snapshots.into_iter().enumerate() {
        let expected = index as u64 + 1;
        if snap.version != expected {
            return Err(format!(
                "快照版本号未从 1 连续递增: 期望 {expected}, 实际 {}",
                snap.version
            ));
        }
        if snap.total != snap.records.len() {
            return Err(format!(
                "快照 {} 记录数与实际不符: 声明 {}, 实际 {}",
                snap.version,
                snap.total,
                snap.records.len()
            ));
        }
        snapshots.push(Snapshot {
            version: snap.version,
            records: snap.records,
        });
    }

    Ok(Store {
        dataset: persisted.dataset,
        snapshots,
    })
}

/// 正常退出时把当前状态原子写入 VDE_DATA：先写同目录临时文件并 fsync，
/// 再改名覆盖目标文件，任何一步失败都不会留下半写的目标文件。
fn save_store(path: &str, store: &Store) -> Result<(), String> {
    let persisted = PersistedState {
        dataset: store.dataset.clone(),
        snapshots: store
            .snapshots
            .iter()
            .map(|s| PersistedSnapshot {
                version: s.version,
                total: s.records.len(),
                records: s.records.clone(),
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&persisted).map_err(|e| format!("序列化状态失败: {e}"))?;

    let tmp_path = format!("{path}.tmp");
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    })();
    if result.is_err() {
        // 清理临时文件；目标文件保持原内容不变。
        let _ = std::fs::remove_file(&tmp_path);
    }
    result.map_err(|e| format!("写入失败: {e}"))
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

    // 全部合法：持锁整体覆盖写入。任一条非法都在上面提前返回，数据集保持原状。
    let mut store = state.store.lock().unwrap();
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
async fn save_version(State(state): State<AppState>) -> Json<SaveResponse> {
    let mut store: MutexGuard<Store> = state.store.lock().unwrap();
    let version = store.snapshots.len() as u64 + 1;
    let records = store
        .dataset
        .iter()
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    let total = store.dataset.len();
    store.snapshots.push(Snapshot { version, records });
    Json(SaveResponse { version, total })
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
    // VDE_DATA 必须提供：缺失或为空时以退出码 2 结束，不监听端口。
    let data_path = match env::var("VDE_DATA") {
        Ok(path) if !path.is_empty() => path,
        _ => {
            eprintln!("错误: 缺少环境变量 VDE_DATA（持久化文件路径）");
            std::process::exit(2);
        }
    };

    // 启动时恢复上次持久化的状态；文件非法时以退出码 1 结束，不监听端口。
    let store = match load_store(&data_path) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("错误: 无法从 {data_path} 恢复状态: {err}");
            std::process::exit(1);
        }
    };

    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/snapshots", get(get_snapshot))
        .with_state(state.clone());

    println!("Listening on http://{}", listener.local_addr()?);
    // Ctrl-C 触发正常退出：先停止接收新请求并等待在途请求完成。
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // 在途请求已全部完成，把数据集与全部快照原子落盘；失败时以退出码 1 结束。
    let store = state.store.lock().unwrap();
    if let Err(err) = save_store(&data_path, &store) {
        eprintln!("错误: 状态落盘到 {data_path} 失败: {err}");
        std::process::exit(1);
    }
    Ok(())
}
