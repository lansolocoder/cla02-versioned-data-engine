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
    env, fs,
    io::{self, ErrorKind, Write},
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

/// 落盘文件结构（内部格式，不是对外契约）。
/// 冗余保存计数字段，恢复时用于发现内容与状态不符的损坏文件。
#[derive(Serialize, Deserialize)]
struct PersistedFile {
    /// 数据集记录数，必须等于 dataset 实际条数。
    dataset_count: usize,
    dataset: Dataset,
    /// 快照总数，必须等于 snapshots 实际条数。
    snapshot_count: usize,
    /// 最大快照版本号，必须等于 snapshot_count（版本号从 1 连续递增）。
    latest_version: u64,
    snapshots: Vec<PersistedSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u64,
    /// 该快照记录数，必须等于 records 实际条数。
    record_count: usize,
    records: BTreeMap<String, Value>,
}

/// 从 VDE_DATA 恢复状态。
///
/// - 文件不存在：按空数据集、零快照启动。
/// - 文件存在但无法读取/解析/内容与状态不符：返回错误，调用方以退出码 1 结束。
fn load_store(path: &Path) -> io::Result<Store> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Store::default()),
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("读取持久化文件 {} 失败: {e}", path.display()),
            ));
        }
    };

    let persisted: PersistedFile = serde_json::from_slice(&bytes).map_err(|e| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("持久化文件 {} 格式非法: {e}", path.display()),
        )
    })?;

    let mut store = Store::default();

    if persisted.dataset_count != persisted.dataset.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "持久化文件 {} 内容与状态不符: dataset_count={} 但实际记录数={}",
                path.display(),
                persisted.dataset_count,
                persisted.dataset.len()
            ),
        ));
    }
    store.dataset = persisted.dataset;

    if persisted.snapshot_count != persisted.snapshots.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "持久化文件 {} 内容与状态不符: snapshot_count={} 但实际快照数={}",
                path.display(),
                persisted.snapshot_count,
                persisted.snapshots.len()
            ),
        ));
    }
    if persisted.latest_version != persisted.snapshots.len() as u64 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "持久化文件 {} 内容与状态不符: latest_version={} 但实际快照数={}",
                path.display(),
                persisted.latest_version,
                persisted.snapshots.len()
            ),
        ));
    }

    for (index, snapshot) in persisted.snapshots.into_iter().enumerate() {
        // 版本号必须从 1 起连续递增。
        let expected_version = index as u64 + 1;
        if snapshot.version != expected_version {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "持久化文件 {} 内容与状态不符: 第 {} 份快照版本号为 {}，应为 {}",
                    path.display(),
                    index,
                    snapshot.version,
                    expected_version
                ),
            ));
        }
        if snapshot.record_count != snapshot.records.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "持久化文件 {} 内容与状态不符: 版本 {} 快照 record_count={} 但实际记录数={}",
                    path.display(),
                    snapshot.version,
                    snapshot.record_count,
                    snapshot.records.len()
                ),
            ));
        }
        store.snapshots.push(Snapshot {
            version: snapshot.version,
            records: snapshot.records,
        });
    }

    Ok(store)
}

/// 把当前数据集与全部快照原子写入 path：先写同目录临时文件，再 rename 覆盖。
///
/// 任一步失败都返回错误且不动原文件（rename 前的失败只影响临时文件）。
fn save_store(path: &Path, store: &Store) -> io::Result<()> {
    let mut snapshots = Vec::with_capacity(store.snapshots.len());
    for snapshot in &store.snapshots {
        snapshots.push(PersistedSnapshot {
            version: snapshot.version,
            record_count: snapshot.records.len(),
            records: snapshot.records.clone(),
        });
    }
    let persisted = PersistedFile {
        dataset_count: store.dataset.len(),
        dataset: store.dataset.clone(),
        snapshot_count: store.snapshots.len(),
        latest_version: store.snapshots.len() as u64,
        snapshots,
    };
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|e| io::Error::new(ErrorKind::Other, format!("序列化持久化数据失败: {e}")))?;

    // 临时文件与目标文件同目录，保证 rename 是原子操作。
    let mut tmp_name = path
        .file_name()
        .map(|name| {
            let mut n = name.to_owned();
            n.push(".tmp");
            n
        })
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("持久化路径 {} 不是有效文件路径", path.display()),
            )
        })?;
    tmp_name.push(format!(".{}", std::process::id()));
    let tmp_path: PathBuf = path.with_file_name(tmp_name);

    {
        let mut file = fs::File::create(&tmp_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("创建临时文件 {} 失败: {e}", tmp_path.display()),
            )
        })?;
        if let Err(e) = file.write_all(&bytes) {
            let _ = fs::remove_file(&tmp_path);
            return Err(io::Error::new(
                e.kind(),
                format!("写入临时文件 {} 失败: {e}", tmp_path.display()),
            ));
        }
        if let Err(e) = file.sync_all() {
            let _ = fs::remove_file(&tmp_path);
            return Err(io::Error::new(
                e.kind(),
                format!("同步临时文件 {} 失败: {e}", tmp_path.display()),
            ));
        }
    }

    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        io::Error::new(
            e.kind(),
            format!(
                "替换持久化文件 {}（临时文件 {}）失败: {e}",
                path.display(),
                tmp_path.display()
            ),
        )
    })?;

    // 尽力 fsync 父目录，让改名结果落盘；失败不影响“文件内容原子可见”的保证。
    if let Some(dir) = path.parent() {
        if let Ok(dir_handle) = fs::File::open(dir) {
            let _ = dir_handle.sync_all();
        }
    }
    Ok(())
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
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
async fn main() {
    // VDE_DATA 必须提供：缺失时不监听端口，以退出码 2 结束。
    let data_path = match env::var("VDE_DATA") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => {
            eprintln!("错误: 必须通过环境变量 VDE_DATA 指定持久化文件路径");
            std::process::exit(2);
        }
    };

    // 启动恢复：文件不存在按空状态启动；文件非法或与状态不符时以退出码 1 结束。
    let store = match load_store(&data_path) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("错误: 从 {} 恢复状态失败: {e}", data_path.display());
            std::process::exit(1);
        }
    };

    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("错误: 监听 {bind} 失败: {e}");
            std::process::exit(1);
        }
    };
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(store)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/snapshots", get(get_snapshot))
        .with_state(state.clone());

    println!("Listening on http://{}", listener.local_addr().unwrap());
    // graceful shutdown 触发后：停止接收新连接/新请求，等待全部在途请求完成。
    let server = axum::serve(listener, app).with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    });
    if let Err(e) = server.await {
        eprintln!("错误: 服务异常: {e}");
        std::process::exit(1);
    }

    // 所有在途请求已结束，持锁取当前状态并原子落盘。
    {
        let store = state.store.lock().unwrap();
        if let Err(e) = save_store(&data_path, &store) {
            eprintln!(
                "错误: 退出时落盘到 {} 失败: {e}（原文件内容保持不变）",
                data_path.display()
            );
            std::process::exit(1);
        }
    }
    // 落盘成功，正常退出。
}
