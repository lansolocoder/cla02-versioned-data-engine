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
    fs,
    io::{self, Write},
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

struct Store {
    dataset: Dataset,
    /// 已保存快照，按版本号严格递增排列；恢复时跳过损坏条目后版本号可能不连续。
    snapshots: Vec<Snapshot>,
    /// 持久化根目录（来自环境变量 VDE_DATA_DIR）。
    data_dir: PathBuf,
}

#[derive(Clone)]
struct AppState {
    /// 所有读写都经过同一把锁，保证批次写入与快照保存对并发交错整体可见。
    store: std::sync::Arc<Mutex<Store>>,
}

// ---------- 持久化 ----------

/// CRC32 (IEEE) 校验和，用于识别半写或损坏的落盘条目。
const CRC_TABLE: [u32; 256] = build_crc_table();

const fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in bytes {
        c = CRC_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

/// 每个已提交版本对应目录下一个 `v<N>.json` 文件。
fn version_file_name(version: u64) -> String {
    format!("v{version}.json")
}

fn parse_version_file_name(name: &str) -> Option<u64> {
    name.strip_prefix('v')?.strip_suffix(".json")?.parse().ok()
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// 从落盘文件反序列化出的单条条目：版本记录、快照内容与当时的数据集。
#[derive(Deserialize)]
struct StoredPayload {
    version: u64,
    dataset: Dataset,
    snapshot: BTreeMap<String, Fields>,
}

/// 原子提交一个版本：版本记录、快照与当时的数据集一起写入 `v<N>.json`。
///
/// 先写临时文件并 fsync，再 rename 成正式文件并 fsync 目录。rename 之前崩溃或
/// 失败时磁盘上只可能残留临时文件，正式条目不会出现，版本号因此可以复用。
fn commit_version(
    dir: &Path,
    version: u64,
    dataset: &Dataset,
    snapshot: &BTreeMap<String, Value>,
) -> io::Result<()> {
    // serde_json 的 Map 按键排序，序列化结果确定，校验和可稳定复算。
    let payload_json = serde_json::to_string(&serde_json::json!({
        "version": version,
        "dataset": dataset,
        "snapshot": snapshot,
    }))
    .map_err(|e| invalid_data(format!("序列化版本 {version} 失败: {e}")))?;
    let content = format!(
        "{{\"payload\":{payload_json},\"checksum\":\"{:08x}\"}}",
        crc32(payload_json.as_bytes())
    );

    let final_path = dir.join(version_file_name(version));
    let tmp_path = final_path.with_extension("json.tmp");
    let result = (|| -> io::Result<()> {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &final_path)?;
        // fsync 目录本身，确保 rename 的结果在崩溃后仍可见。
        fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        // 失败不留痕迹：清掉可能只写了一半的临时文件。
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

/// 读取并校验一个落盘条目；任何不完整或校验不通过都返回 Err。
fn load_entry(path: &Path) -> io::Result<StoredPayload> {
    let bytes = fs::read(path)?;
    let file: Value = serde_json::from_slice(&bytes)
        .map_err(|e| invalid_data(format!("条目不是合法 JSON: {e}")))?;
    let payload = file
        .get("payload")
        .ok_or_else(|| invalid_data("条目缺少 payload"))?;
    let checksum = file
        .get("checksum")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_data("条目缺少 checksum"))?;
    let payload_json = serde_json::to_string(payload)
        .map_err(|e| invalid_data(format!("无法重新序列化 payload: {e}")))?;
    if checksum != format!("{:08x}", crc32(payload_json.as_bytes())) {
        return Err(invalid_data("条目校验和不匹配"));
    }
    serde_json::from_value(payload.clone())
        .map_err(|e| invalid_data(format!("条目结构非法: {e}")))
}

/// 启动时从数据目录恢复状态：返回（当前数据集，已保存快照列表）。
fn recover(dir: &Path) -> io::Result<(Dataset, Vec<Snapshot>)> {
    let mut committed: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(stem) = name.strip_suffix(".tmp") {
            // 上次提交未完成留下的临时文件：按不存在处理并清理。
            if parse_version_file_name(stem).is_some() {
                let _ = fs::remove_file(entry.path());
            }
            continue;
        }
        if let Some(version) = parse_version_file_name(name) {
            committed.push((version, entry.path()));
        }
    }
    committed.sort_by_key(|&(version, _)| version);

    let mut dataset = Dataset::new();
    let mut snapshots: Vec<Snapshot> = Vec::new();
    for (file_version, path) in committed {
        match load_entry(&path) {
            Ok(stored)
                if stored.version == file_version
                    && snapshots.last().is_none_or(|s| stored.version > s.version) =>
            {
                dataset = stored.dataset;
                snapshots.push(Snapshot {
                    version: stored.version,
                    records: stored
                        .snapshot
                        .into_iter()
                        .map(|(key, fields)| (key, Value::Object(fields)))
                        .collect(),
                });
            }
            // 半写或校验不通过的条目按不存在处理并跳过；其后完整且校验通过
            // 的条目照常恢复，版本号按持久化顺序严格递增。
            Err(_) => continue,
            // 校验通过但版本号与文件名或持久化顺序矛盾：无法确定其后条目是否
            // 完整，以最后一个完整条目为准，其后一律丢弃。
            Ok(_) => break,
        }
    }
    Ok((dataset, snapshots))
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

/// POST /versions：把当前数据集固化为不可变快照。
///
/// 先把版本记录、快照与当时的数据集原子落盘，成功后才提交内存状态。
/// 落盘失败时整体失败：返回 500，数据集与版本号都保持不变。
async fn save_version(
    State(state): State<AppState>,
) -> Result<Json<SaveResponse>, (StatusCode, Json<ErrorBody>)> {
    let mut store: MutexGuard<Store> = state.store.lock().unwrap();
    // 版本号取自最后一份已保存快照加 1：提交失败时不递增，下次保存复用同一号码。
    let version = store.snapshots.last().map_or(1, |s| s.version + 1);
    let records: BTreeMap<String, Value> = store
        .dataset
        .iter()
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    let total = records.len();
    commit_version(&store.data_dir, version, &store.dataset, &records).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("保存版本 {version} 时落盘失败: {e}"),
                index: None,
            }),
        )
    })?;
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
    // 持久化根目录必须由 VDE_DATA_DIR 显式给出；未设置或为空视为配置错误。
    let data_dir = match env::var("VDE_DATA_DIR") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => {
            eprintln!("配置错误: 环境变量 VDE_DATA_DIR 未设置或为空，服务退出");
            std::process::exit(1);
        }
    };
    match fs::metadata(&data_dir) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            // 指向普通文件等非目录路径：报错退出，不覆盖也不删除它。
            eprintln!(
                "配置错误: VDE_DATA_DIR 指向的路径不是目录: {}",
                data_dir.display()
            );
            std::process::exit(1);
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if let Err(e) = fs::create_dir_all(&data_dir) {
                eprintln!("配置错误: 无法创建数据目录 {}: {e}", data_dir.display());
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("配置错误: 无法访问数据目录 {}: {e}", data_dir.display());
            std::process::exit(1);
        }
    }

    // 加载已持久化状态；目录中没有持久化文件时即全新实例，版本号从 1 开始。
    let (dataset, snapshots) = match recover(&data_dir) {
        Ok(recovered) => recovered,
        Err(e) => {
            eprintln!("无法从 {} 加载持久化状态: {e}", data_dir.display());
            std::process::exit(1);
        }
    };
    if let Some(last) = snapshots.last() {
        println!(
            "已从 {} 恢复 {} 个版本，下一版本号为 {}",
            data_dir.display(),
            snapshots.len(),
            last.version + 1
        );
    }

    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: std::sync::Arc::new(Mutex::new(Store {
            dataset,
            snapshots,
            data_dir,
        })),
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
