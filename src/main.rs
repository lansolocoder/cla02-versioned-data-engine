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

/// 把查询参数值解析为正整数版本号。
fn parse_version_value(name: &str, value: &str) -> Result<u64, String> {
    let version: u64 = value
        .parse()
        .map_err(|_| format!("{name} 必须是正整数: {value}"))?;
    if version == 0 {
        return Err(format!("{name} 必须是正整数: {value}"));
    }
    Ok(version)
}

/// 手动解析比较查询串（percent-decode 后取 from / to，均为必选正整数）。
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
            match name.as_ref() {
                "from" => from = Some(parse_version_value("from", &value)?),
                "to" => to = Some(parse_version_value("to", &value)?),
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

/// GET /snapshots/diff?from=N&to=M：比较两份已保存快照的记录级变化。
async fn diff_snapshots(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<DiffResponse>, (StatusCode, Json<ErrorBody>)> {
    let params = parse_diff_query(req.uri()).map_err(bad_request)?;

    // 只读比较：持锁期间取出两份快照的克隆，不创建、不修改任何快照。
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
    drop(store);

    // 两份快照的记录都按键字典序排列，归并迭代即得键并集的有序变化列表。
    let mut changes = Vec::new();
    let mut from_iter = from_snapshot.records.iter().peekable();
    let mut to_iter = to_snapshot.records.iter().peekable();
    loop {
        // 先只读地比较两侧当前键，决定消费哪一侧，避免借用冲突。
        let ordering = match (from_iter.peek(), to_iter.peek()) {
            (Some((from_key, _)), Some((to_key, _))) => from_key.cmp(to_key),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => break,
        };
        match ordering {
            std::cmp::Ordering::Less => {
                let (key, fields) = from_iter.next().expect("peeked");
                changes.push(ChangeOut {
                    key: key.clone(),
                    change: "removed",
                    fields: fields.clone(),
                });
            }
            std::cmp::Ordering::Greater => {
                let (key, fields) = to_iter.next().expect("peeked");
                changes.push(ChangeOut {
                    key: key.clone(),
                    change: "added",
                    fields: fields.clone(),
                });
            }
            std::cmp::Ordering::Equal => {
                let (key, from_fields) = from_iter.next().expect("peeked");
                let (_, to_fields) = to_iter.next().expect("peeked");
                // Value 相等按 JSON 值语义，与对象内字段顺序无关。
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
                    key: key.clone(),
                    change,
                    fields,
                });
            }
        }
    }

    Ok(Json(DiffResponse {
        from: params.from,
        to: params.to,
        changes,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(Store::default())),
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
