use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    error::Error,
    sync::{Arc, RwLock},
};

// ---------- 错误 ----------

#[derive(Debug, Clone, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

// ---------- 存储 ----------

/// 单个版本的完整记录。Rust 的 `String` 按 Unicode 码点（标量值）排序，
/// 因此迭代顺序即为要求的 key 码点顺序。
type Records = BTreeMap<String, Value>;

/// 提交内容的规范化形式，用于幂等比较。
#[derive(Debug, PartialEq)]
struct CommitContent {
    upserts: BTreeMap<String, Value>,
    deletes: BTreeSet<String>,
}

#[derive(Default)]
struct Dataset {
    /// 只增不改；`snapshots[v - 1]` 即版本 v 的快照，每个快照持有独立的记录副本。
    snapshots: Vec<Records>,
    /// request_id ->（首次提交产生的版本, 首次提交内容）
    requests: HashMap<String, (u64, CommitContent)>,
}

#[derive(Default)]
struct Store {
    datasets: BTreeMap<String, Dataset>,
}

#[derive(Clone)]
struct AppState {
    // 临界区内只有内存计算、不跨 .await，std 锁即可：
    // 写锁串行化所有提交，读锁保证读者只看到完整的提交前/后状态。
    store: Arc<RwLock<Store>>,
}

// ---------- 提交核心逻辑 ----------

#[derive(Debug)]
struct Validated {
    request_id: String,
    base: Option<u64>,
    content: CommitContent,
}

fn validate_commit(name: &str, req: CommitRequest) -> Result<Validated, ApiError> {
    if name.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "数据集名称不能为空",
        ));
    }
    if req.request_id.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "request_id 不能为空",
        ));
    }

    let mut upserts: BTreeMap<String, Value> = BTreeMap::new();
    for item in req.upserts {
        if item.key.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "upserts 中存在空 key",
            ));
        }
        if upserts.insert(item.key.clone(), item.value).is_some() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("upserts 中存在重复 key：{}", item.key),
            ));
        }
    }

    let mut deletes: BTreeSet<String> = BTreeSet::new();
    for key in req.deletes {
        if key.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "deletes 中存在空 key",
            ));
        }
        if !deletes.insert(key.clone()) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("deletes 中存在重复 key：{key}"),
            ));
        }
        if upserts.contains_key(&key) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("key 不能同时出现在 upserts 和 deletes 中：{key}"),
            ));
        }
    }

    if upserts.is_empty() && deletes.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "upserts 和 deletes 不能同时为空（空变更）",
        ));
    }

    Ok(Validated {
        request_id: req.request_id,
        base: req.base,
        content: CommitContent { upserts, deletes },
    })
}

fn apply_change(records: &mut Records, content: &CommitContent) {
    for (key, value) in &content.upserts {
        records.insert(key.clone(), value.clone());
    }
    for key in &content.deletes {
        records.remove(key);
    }
}

/// 在已有写锁内执行提交。失败时不做任何改动。
fn commit(store: &mut Store, name: &str, input: Validated) -> Result<u64, ApiError> {
    let Validated {
        request_id,
        base,
        content,
    } = input;

    // 幂等检查先于 base 检查：同一 request_id 重放时，即使 base 已过期也要返回首次结果。
    if let Some(dataset) = store.datasets.get(name)
        && let Some((version, original)) = dataset.requests.get(&request_id)
    {
        if original == &content {
            return Ok(*version);
        }
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            format!("request_id {request_id} 已用于内容不同的提交"),
        ));
    }

    match base {
        None => {
            // 首次提交：数据集必须尚不存在。
            if store.datasets.contains_key(name) {
                let current = store.datasets[name].snapshots.len() as u64;
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "stale_base",
                    format!("数据集已存在（当前版本 {current}），首次提交 base 必须为 null"),
                ));
            }
            let mut records = Records::new();
            apply_change(&mut records, &content);
            let mut dataset = Dataset {
                snapshots: vec![records],
                requests: HashMap::new(),
            };
            dataset.requests.insert(request_id, (1, content));
            store.datasets.insert(name.to_owned(), dataset);
            Ok(1)
        }
        Some(base) => {
            let Some(dataset) = store.datasets.get_mut(name) else {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "stale_base",
                    format!("数据集 {name} 不存在，或 base {base} 已过期"),
                ));
            };
            let current = dataset.snapshots.len() as u64;
            if base != current {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "stale_base",
                    format!("base {base} 已过期，当前版本为 {current}"),
                ));
            }

            // 从最新快照克隆出独立副本再修改，历史快照保持不变。
            let mut records = dataset.snapshots.last().expect("非空数据集").clone();
            apply_change(&mut records, &content);
            dataset.snapshots.push(records);
            let version = current + 1;
            dataset.requests.insert(request_id, (version, content));
            Ok(version)
        }
    }
}

// ---------- 请求 / 响应结构 ----------

#[derive(Deserialize)]
struct CommitRequest {
    base: Option<u64>,
    request_id: String,
    #[serde(default)]
    upserts: Vec<UpsertEntry>,
    #[serde(default)]
    deletes: Vec<String>,
}

#[derive(Deserialize)]
struct UpsertEntry {
    key: String,
    value: Value,
}

#[derive(Serialize)]
struct CommitResponse {
    dataset: String,
    version: u64,
    request_id: String,
}

#[derive(Serialize)]
struct RecordOut {
    key: String,
    value: Value,
}

#[derive(Serialize)]
struct SnapshotResponse {
    dataset: String,
    version: u64,
    records: Vec<RecordOut>,
}

// ---------- HTTP 处理 ----------

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn version() -> Json<Version> {
    Json(Version {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn commit_dataset(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // 分两步解析：语法错误（畸形 JSON）与数据错误（字段类型不对/缺失）区分 code。
    let value: Value = serde_json::from_slice(&body).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            format!("请求体不是合法的 JSON：{e}"),
        )
    })?;
    let req: CommitRequest = serde_json::from_value(value).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            format!("请求字段无效：{e}"),
        )
    })?;

    let input = validate_commit(&name, req)?;
    let request_id = input.request_id.clone();

    let version = {
        let mut store = state
            .store
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        commit(&mut store, &name, input)?
    };

    Ok((
        StatusCode::CREATED,
        Json(CommitResponse {
            dataset: name,
            version,
            request_id,
        }),
    )
        .into_response())
}

async fn get_snapshot(
    State(state): State<AppState>,
    Path((name, version_raw)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    let store = state
        .store
        .read()
        .unwrap_or_else(|poison| poison.into_inner());

    let Some(dataset) = store.datasets.get(&name) else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "dataset_not_found",
            format!("数据集不存在：{name}"),
        ));
    };

    let latest = dataset.snapshots.len() as u64;
    let version = if version_raw == "latest" {
        latest
    } else {
        version_raw.parse::<u64>().map_err(|_| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "snapshot_not_found",
                format!("数据集 {name} 不存在版本 {version_raw}"),
            )
        })?
    };

    let Some(records) = version
        .checked_sub(1)
        .and_then(|index| dataset.snapshots.get(index as usize))
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "snapshot_not_found",
            format!("数据集 {name} 不存在版本 {version_raw}"),
        ));
    };

    Ok(Json(SnapshotResponse {
        dataset: name,
        version,
        records: records
            .iter()
            .map(|(key, value)| RecordOut {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    }))
}

async fn fallback() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "接口不存在")
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "该路径不支持此请求方法",
    )
}

// ---------- 启动 ----------

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;

    let state = AppState {
        store: Arc::new(RwLock::new(Store::default())),
    };

    let app = Router::new()
        .route("/health", get(health).fallback(method_not_allowed))
        .route("/version", get(version).fallback(method_not_allowed))
        .route(
            "/datasets/{name}/commits",
            post(commit_dataset).fallback(method_not_allowed),
        )
        .route(
            "/datasets/{name}/snapshots/{version}",
            get(get_snapshot).fallback(method_not_allowed),
        )
        .fallback(fallback)
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

// ---------- 测试 ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        base: Option<u64>,
        request_id: &str,
        upserts: &[(&str, Value)],
        deletes: &[&str],
    ) -> Validated {
        let req = CommitRequest {
            base,
            request_id: request_id.to_owned(),
            upserts: upserts
                .iter()
                .map(|(k, v)| UpsertEntry {
                    key: (*k).to_owned(),
                    value: v.clone(),
                })
                .collect(),
            deletes: deletes.iter().map(|k| (*k).to_owned()).collect(),
        };
        validate_commit("ds", req).unwrap()
    }

    fn upsert(
        store: &mut Store,
        base: Option<u64>,
        rid: &str,
        pairs: &[(&str, i64)],
        deletes: &[&str],
    ) -> Result<u64, ApiError> {
        let pairs: Vec<(String, Value)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), Value::from(*v)))
            .collect();
        let pairs_ref: Vec<(&str, Value)> =
            pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        commit(store, "ds", input(base, rid, &pairs_ref, deletes))
    }

    fn keys(store: &Store, version: u64) -> Vec<String> {
        store.datasets["ds"].snapshots[(version - 1) as usize]
            .keys()
            .cloned()
            .collect()
    }

    #[test]
    fn versions_increment_from_one_and_snapshots_are_immutable() {
        let mut store = Store::default();
        assert_eq!(
            upsert(&mut store, None, "r1", &[("a", 1), ("b", 2)], &[]).unwrap(),
            1
        );
        assert_eq!(
            upsert(&mut store, Some(1), "r2", &[("a", 10)], &[]).unwrap(),
            2
        );
        assert_eq!(upsert(&mut store, Some(2), "r3", &[], &["b"]).unwrap(), 3);

        assert_eq!(keys(&store, 1), vec!["a", "b"]);
        assert_eq!(keys(&store, 2), vec!["a", "b"]);
        assert_eq!(keys(&store, 3), vec!["a"]);
        assert_eq!(store.datasets["ds"].snapshots[0]["a"], Value::from(1));
        assert_eq!(store.datasets["ds"].snapshots[2]["a"], Value::from(10));
    }

    #[test]
    fn stale_base_conflict_makes_no_change() {
        let mut store = Store::default();
        upsert(&mut store, None, "r1", &[("a", 1)], &[]).unwrap();
        upsert(&mut store, Some(1), "r2", &[("b", 2)], &[]).unwrap();

        let err = upsert(&mut store, Some(1), "r3", &[("c", 3)], &[]).unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.code, "stale_base");
        assert_eq!(store.datasets["ds"].snapshots.len(), 2);

        // null base 对已存在数据集也是冲突
        let err = upsert(&mut store, None, "r4", &[("c", 3)], &[]).unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.code, "stale_base");
        assert_eq!(store.datasets["ds"].snapshots.len(), 2);
    }

    #[test]
    fn idempotent_replay_returns_first_version_even_with_stale_base() {
        let mut store = Store::default();
        let first = vec![("a", Value::from(1)), ("b", Value::from(2))];
        assert_eq!(
            commit(&mut store, "ds", input(None, "rid-x", &first, &[])).unwrap(),
            1
        );
        // 推进到版本 2，使原 base（null）过期
        upsert(&mut store, Some(1), "other", &[("c", 3)], &[]).unwrap();

        // 同样内容重放，且 upserts 数组顺序不同（规范化为 BTreeMap 后等价）：
        // 返回首次版本 1，不新增版本。
        let replay = vec![("b", Value::from(2)), ("a", Value::from(1))];
        assert_eq!(
            commit(&mut store, "ds", input(None, "rid-x", &replay, &[])).unwrap(),
            1
        );
        assert_eq!(store.datasets["ds"].snapshots.len(), 2);
    }

    #[test]
    fn same_request_id_different_content_conflicts() {
        let mut store = Store::default();
        let p1 = vec![("a", Value::from(1))];
        commit(&mut store, "ds", input(None, "rid", &p1, &[])).unwrap();
        let p2 = vec![("a", Value::from(2))];
        let err = commit(&mut store, "ds", input(None, "rid", &p2, &[])).unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.code, "idempotency_conflict");
        assert_eq!(store.datasets["ds"].snapshots.len(), 1);
    }

    #[test]
    fn validation_errors() {
        let bad = validate_commit(
            "ds",
            CommitRequest {
                base: None,
                request_id: "r".to_owned(),
                upserts: vec![],
                deletes: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(bad.status, StatusCode::BAD_REQUEST);

        let bad = validate_commit(
            "ds",
            CommitRequest {
                base: None,
                request_id: "".to_owned(),
                upserts: vec![UpsertEntry {
                    key: "a".to_owned(),
                    value: Value::Null,
                }],
                deletes: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(bad.code, "invalid_request");

        let dup = [("a", Value::from(1)), ("a", Value::from(2))];
        assert_eq!(
            validate_commit(
                "ds",
                CommitRequest {
                    base: None,
                    request_id: "r".to_owned(),
                    upserts: dup
                        .iter()
                        .map(|(k, v)| UpsertEntry {
                            key: (*k).to_owned(),
                            value: v.clone()
                        })
                        .collect(),
                    deletes: vec![],
                }
            )
            .unwrap_err()
            .code,
            "invalid_request"
        );

        let overlap = [("a", Value::from(1))];
        assert!(
            validate_commit(
                "ds",
                CommitRequest {
                    base: None,
                    request_id: "r".to_owned(),
                    upserts: overlap
                        .iter()
                        .map(|(k, v)| UpsertEntry {
                            key: (*k).to_owned(),
                            value: v.clone()
                        })
                        .collect(),
                    deletes: vec!["a".to_owned()],
                }
            )
            .is_err()
        );
    }

    #[test]
    fn unicode_code_point_ordering() {
        let mut store = Store::default();
        // "中"(U+4E2D) > 所有拉丁；大写 A < 小写 a；é(U+00E9) < z
        let pairs = vec![
            ("中", Value::from(1)),
            ("a", Value::from(1)),
            ("A", Value::from(1)),
            ("z", Value::from(1)),
            ("é", Value::from(1)),
        ];
        commit(&mut store, "ds", input(None, "r1", &pairs, &[])).unwrap();
        assert_eq!(keys(&store, 1), vec!["A", "a", "z", "é", "中"]);
    }

    #[test]
    fn request_ids_are_scoped_per_dataset() {
        let mut store = Store::default();
        let p = vec![("a", Value::from(1))];
        assert_eq!(
            commit(&mut store, "d1", input(None, "rid", &p, &[])).unwrap(),
            1
        );
        assert_eq!(
            commit(&mut store, "d2", input(None, "rid", &p, &[])).unwrap(),
            1
        );
    }
}
