mod persistence;

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
    path::PathBuf,
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
    records: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Store {
    dataset: Dataset,
    /// 已保存快照；恢复时版本号允许有缺号，因此不能再用长度推导版本。
    snapshots: Vec<Snapshot>,
    /// 下一个可分配的版本号，全新实例从 1 开始。
    next_version: u64,
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: std::sync::Arc<Mutex<Store>>,
    /// 持久化根目录（来自 VDE_DATA_DIR，启动时已校验存在且为目录）。
    data_dir: PathBuf,
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

/// GET /snapshots/diff 查询参数：必选 from 与 to。
struct DiffParams {
    from: u64,
    to: u64,
}

#[derive(Serialize)]
struct ChangeOut {
    key: String,
    change: &'static str,
    fields: Value,
}

#[derive(Serialize)]
struct DiffResponse {
    from: u64,
    to: u64,
    changes: Vec<ChangeOut>,
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

/// 手动解析比较查询串（percent-decode 后取 from / to，均必须是正整数）。
fn parse_diff_query(uri: &Uri) -> Result<DiffParams, String> {
    let mut from: Option<u64> = None;
    let mut to: Option<u64> = None;
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            let Some((raw_name, raw_value)) = pair.split_once('=') else {
                return Err(format!("非法查询参数: {pair}"));
            };
            let name = percent_decode_str(raw_name).decode_utf8_lossy();
            let value = percent_decode_str(raw_value).decode_utf8_lossy();
            let parse_version = |label: &str| -> Result<u64, String> {
                let parsed: u64 = value
                    .parse()
                    .map_err(|_| format!("{label} 必须是正整数: {value}"))?;
                if parsed == 0 {
                    return Err(format!("{label} 必须是正整数: {value}"));
                }
                Ok(parsed)
            };
            match name.as_ref() {
                "from" => from = Some(parse_version("from")?),
                "to" => to = Some(parse_version("to")?),
                other => return Err(format!("未知查询参数: {other}")),
            }
        }
    }
    match (from, to) {
        (Some(from), Some(to)) => Ok(DiffParams { from, to }),
        (None, _) => Err("缺少必填查询参数 from".to_owned()),
        (_, None) => Err("缺少必填查询参数 to".to_owned()),
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

/// POST /versions：把当前数据集固化为不可变快照，并原子落盘。
async fn save_version(
    State(state): State<AppState>,
) -> Result<Json<SaveResponse>, (StatusCode, Json<ErrorBody>)> {
    let mut store: MutexGuard<Store> = state.store.lock().unwrap();
    let version = store.next_version;
    let records = store
        .dataset
        .iter()
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    let total = store.dataset.len();

    // 先落盘、后推进内存：写盘失败时数据集与版本号都保持保存前的状态，
    // 磁盘上也不会留下本次保存的任何痕迹，同一版本号下次可复用。
    persistence::write_entry(&state.data_dir, version, &store.dataset).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("版本 {version} 持久化失败: {e}"),
                index: None,
            }),
        )
    })?;

    store.snapshots.push(Snapshot { version, records });
    store.next_version += 1;
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

/// GET /snapshots/diff?from=N&to=M：比较两份快照的记录级变化。
async fn diff_snapshots(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<DiffResponse>, (StatusCode, Json<ErrorBody>)> {
    let params = parse_diff_query(req.uri()).map_err(bad_request)?;

    let store = state.store.lock().unwrap();
    let find = |version: u64| {
        store
            .snapshots
            .iter()
            .find(|s| s.version == version)
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("版本 {version} 不存在或尚未保存"),
                        index: None,
                    }),
                )
            })
    };
    let from_snapshot = find(params.from)?;
    let to_snapshot = find(params.to)?;

    // 两份快照的 records 都是 BTreeMap，按键归并即得字典序的键并集。
    let mut changes = Vec::new();
    let mut from_iter = from_snapshot.records.iter().peekable();
    let mut to_iter = to_snapshot.records.iter().peekable();
    loop {
        match (from_iter.peek(), to_iter.peek()) {
            (Some(&(from_key, from_fields)), Some(&(to_key, to_fields))) => {
                use std::cmp::Ordering::*;
                match from_key.cmp(to_key) {
                    Less => {
                        changes.push(ChangeOut {
                            key: from_key.clone(),
                            change: "removed",
                            fields: from_fields.clone(),
                        });
                        from_iter.next();
                    }
                    Greater => {
                        changes.push(ChangeOut {
                            key: to_key.clone(),
                            change: "added",
                            fields: to_fields.clone(),
                        });
                        to_iter.next();
                    }
                    Equal => {
                        // serde_json::Value 的相等即 JSON 值语义，与对象内字段顺序无关。
                        let (change, fields) = if from_fields == to_fields {
                            ("unchanged", Value::Null)
                        } else {
                            (
                                "modified",
                                serde_json::json!({
                                    "from": from_fields,
                                    "to": to_fields,
                                }),
                            )
                        };
                        changes.push(ChangeOut {
                            key: from_key.clone(),
                            change,
                            fields,
                        });
                        from_iter.next();
                        to_iter.next();
                    }
                }
            }
            (Some(&(from_key, from_fields)), None) => {
                changes.push(ChangeOut {
                    key: from_key.clone(),
                    change: "removed",
                    fields: from_fields.clone(),
                });
                from_iter.next();
            }
            (None, Some(&(to_key, to_fields))) => {
                changes.push(ChangeOut {
                    key: to_key.clone(),
                    change: "added",
                    fields: to_fields.clone(),
                });
                to_iter.next();
            }
            (None, None) => break,
        }
    }

    Ok(Json(DiffResponse {
        from: from_snapshot.version,
        to: to_snapshot.version,
        changes,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 持久化根目录是必需配置：缺失、为空或指向非目录都视为配置错误，
    // 打印一行说明后以非零码退出，不创建任何目录、不启动服务。
    let data_dir = match env::var("VDE_DATA_DIR") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => {
            eprintln!(
                "配置错误: 必须设置环境变量 VDE_DATA_DIR 指向持久化根目录（当前未设置或为空）"
            );
            process::exit(2);
        }
    };
    match std::fs::metadata(&data_dir) {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => {
            eprintln!(
                "配置错误: VDE_DATA_DIR 指向的不是目录: {}",
                data_dir.display()
            );
            process::exit(2);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Err(e) = std::fs::create_dir_all(&data_dir) {
                eprintln!("配置错误: 无法创建持久化目录 {}: {e}", data_dir.display());
                process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("配置错误: 无法访问持久化目录 {}: {e}", data_dir.display());
            process::exit(2);
        }
    }

    // 加载已持久化状态；目录为空时按全新实例启动（版本号从 1 开始）。
    let loaded = match persistence::load(&data_dir) {
        Ok(loaded) => loaded,
        Err(e) => {
            eprintln!("启动失败: 无法读取持久化目录 {}: {e}", data_dir.display());
            process::exit(2);
        }
    };
    let store = Store {
        dataset: loaded.dataset,
        snapshots: loaded
            .snapshots
            .into_iter()
            .map(|s| Snapshot {
                version: s.version,
                records: s.records,
            })
            .collect(),
        next_version: loaded.next_version,
    };

    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(store)),
        data_dir,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/snapshots", get(get_snapshot))
        .route("/snapshots/diff", get(diff_snapshots))
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
