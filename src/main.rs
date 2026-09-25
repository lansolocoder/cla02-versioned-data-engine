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
    /// 持久化目录；未设置 VDE_DATA_DIR 时为 None，服务纯内存运行。
    persistence: Option<Arc<persist::Persistence>>,
}

/// 磁盘持久化：数据集与快照的原子落盘、启动恢复与目录独占锁。
mod persist {
    use super::{Dataset, Snapshot};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        fmt,
        fs::{self, File, OpenOptions},
        io::{self, Write},
        path::{Path, PathBuf},
    };

    const DATASET_FILE: &str = "dataset.json";
    const LOCK_FILE: &str = "vde.lock";
    const SNAPSHOT_PREFIX: &str = "snapshot-";
    const SNAPSHOT_SUFFIX: &str = ".json";

    /// 快照文件的磁盘格式。
    #[derive(Serialize, Deserialize)]
    struct SnapshotFile {
        version: u64,
        records: BTreeMap<String, Value>,
    }

    /// 打开持久化目录失败的原因。任何情况下调用方都拒绝启动且不改动目录内容。
    pub enum OpenError {
        /// 目录不可创建、不可读写等系统错误。
        Io(io::Error),
        /// 目录已被其他运行中的实例锁定。
        Locked(PathBuf),
        /// 目录中数据损坏或格式无法解析。
        Corrupt(String),
    }

    impl fmt::Display for OpenError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                OpenError::Io(e) => write!(f, "持久化目录不可用: {e}"),
                OpenError::Locked(dir) => {
                    write!(f, "持久化目录已被其他运行中的实例占用: {}", dir.display())
                }
                OpenError::Corrupt(msg) => write!(f, "持久化数据损坏: {msg}"),
            }
        }
    }

    impl From<io::Error> for OpenError {
        fn from(e: io::Error) -> Self {
            OpenError::Io(e)
        }
    }

    /// 持有目录锁的持久化句柄；锁文件在进程退出时由操作系统自动释放。
    pub struct Persistence {
        dir: PathBuf,
        _lock: File,
    }

    impl Persistence {
        /// 打开（必要时创建）持久化目录，获取独占锁，并完整加载、校验已有数据。
        /// 校验失败或目录被占用时返回错误，调用方必须拒绝启动；
        /// 此函数不写入除锁文件外的任何内容，且锁文件不会截断已有数据。
        pub fn open(dir: PathBuf) -> Result<(Self, Dataset, Vec<Snapshot>), OpenError> {
            if !dir.exists() {
                fs::create_dir_all(&dir)?;
            }
            if !dir.is_dir() {
                return Err(OpenError::Corrupt(format!(
                    "持久化路径不是目录: {}",
                    dir.display()
                )));
            }

            // 先取锁再读数据，防止两个实例同时启动时交错读写。
            let lock_path = dir.join(LOCK_FILE);
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)?;
            lock.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => OpenError::Locked(dir.clone()),
                std::fs::TryLockError::Error(io) => OpenError::Io(io),
            })?;

            let dataset = load_dataset(&dir)?;
            let snapshots = load_snapshots(&dir)?;
            Ok((Persistence { dir, _lock: lock }, dataset, snapshots))
        }

        /// 把当前数据集整体原子替换到磁盘（写批次成功后调用）。
        pub fn save_dataset(&self, dataset: &Dataset) -> io::Result<()> {
            let bytes = serde_json::to_vec(dataset)?;
            atomic_write(&self.dir, DATASET_FILE, &bytes)
        }

        /// 把一份新快照原子落盘（保存版本时调用）。成功后该文件不可变。
        pub fn save_snapshot(&self, snapshot: &Snapshot) -> io::Result<()> {
            let file = SnapshotFile {
                version: snapshot.version,
                records: snapshot.records.clone(),
            };
            let bytes = serde_json::to_vec(&file)?;
            atomic_write(
                &self.dir,
                &format!("{SNAPSHOT_PREFIX}{}{SNAPSHOT_SUFFIX}", snapshot.version),
                &bytes,
            )
        }
    }

    /// 原子写入：先写临时文件并 fsync，再 rename 覆盖目标，最后 fsync 目录。
    /// 进程在任意时刻被强杀，目标文件要么保持旧内容、要么是完整的新内容；
    /// 可能残留的临时文件在启动时被忽略。
    fn atomic_write(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
        let tmp_path = dir.join(format!("{name}.tmp"));
        {
            let mut tmp = File::create(&tmp_path)?;
            tmp.write_all(bytes)?;
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, dir.join(name))?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// 读取当前数据集；文件不存在视为空数据集（全新目录）。
    fn load_dataset(dir: &Path) -> Result<Dataset, OpenError> {
        let path = dir.join(DATASET_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Dataset::new()),
            Err(e) => return Err(OpenError::Io(e)),
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| OpenError::Corrupt(format!("{DATASET_FILE} 无法解析: {e}")))
    }

    /// 读取全部快照文件，并校验版本号与文件名一致、从 1 开始严格连续。
    fn load_snapshots(dir: &Path) -> Result<Vec<Snapshot>, OpenError> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(rest) = name.strip_prefix(SNAPSHOT_PREFIX) else {
                continue;
            };
            let Some(number) = rest.strip_suffix(SNAPSHOT_SUFFIX) else {
                continue;
            };
            let file_version: u64 = number
                .parse()
                .map_err(|_| OpenError::Corrupt(format!("快照文件名非法: {name}")))?;
            let bytes = fs::read(entry.path())?;
            let file: SnapshotFile = serde_json::from_slice(&bytes)
                .map_err(|e| OpenError::Corrupt(format!("快照文件 {name} 无法解析: {e}")))?;
            if file.version != file_version {
                return Err(OpenError::Corrupt(format!(
                    "快照文件 {name} 内版本号 {} 与文件名不符",
                    file.version
                )));
            }
            snapshots.push(Snapshot {
                version: file.version,
                records: file.records,
            });
        }
        snapshots.sort_by_key(|s| s.version);
        for (index, snapshot) in snapshots.iter().enumerate() {
            let expected = index as u64 + 1;
            if snapshot.version != expected {
                return Err(OpenError::Corrupt(format!(
                    "快照版本不连续: 期望 {expected}, 实际 {}",
                    snapshot.version
                )));
            }
        }
        Ok(snapshots)
    }
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

/// 持久化失败：磁盘错误不属于请求问题，返回 500，内存状态保持不变。
fn persist_failure(message: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
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
    if let Some(persistence) = &state.persistence {
        // 先落盘后改内存：落盘失败时内存状态不变，客户端可安全重试。
        let mut next = store.dataset.clone();
        for (key, fields) in batch {
            next.insert(key, fields);
        }
        persistence
            .save_dataset(&next)
            .map_err(|e| persist_failure(format!("数据集持久化失败: {e}")))?;
        store.dataset = next;
    } else {
        for (key, fields) in batch {
            store.dataset.insert(key, fields);
        }
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
        .map(|(key, fields)| (key.clone(), Value::Object(fields.clone())))
        .collect();
    let snapshot = Snapshot { version, records };
    if let Some(persistence) = &state.persistence {
        // 原子落盘成功后才推进内存中的版本号；失败或崩溃都不会产生半截快照或跳号。
        persistence
            .save_snapshot(&snapshot)
            .map_err(|e| persist_failure(format!("快照持久化失败: {e}")))?;
    }
    let total = store.dataset.len();
    store.snapshots.push(snapshot);
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
    // 先完成持久化目录的锁定与数据恢复，再绑定端口；
    // 恢复失败时拒绝启动，不持有端口也不写任何数据。
    let (store, persistence) = match env::var_os("VDE_DATA_DIR") {
        Some(dir) => match persist::Persistence::open(dir.into()) {
            Ok((persistence, dataset, snapshots)) => {
                let versions = snapshots.len();
                if versions > 0 {
                    println!(
                        "已从持久化目录恢复 {} 条记录、{versions} 份快照",
                        dataset.len()
                    );
                }
                (Store { dataset, snapshots }, Some(Arc::new(persistence)))
            }
            Err(e) => {
                eprintln!("无法启动: {e}");
                std::process::exit(1);
            }
        },
        None => (Store::default(), None),
    };

    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        persistence,
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
