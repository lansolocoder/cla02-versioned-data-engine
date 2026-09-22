use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    error::Error,
    sync::{Arc, Mutex},
};

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
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

// ---------------------------------------------------------------------------
// Error type: every failure is JSON with a stable `code` and a readable
// `message`.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

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

    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
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

// ---------------------------------------------------------------------------
// Commit request model
// ---------------------------------------------------------------------------

/// Upsert map that rejects duplicate keys instead of silently keeping the
/// last one (serde_json's default behaviour for `Value`/`Map`).
#[derive(Debug, Clone, PartialEq)]
struct Upserts(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for Upserts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UpsertsVisitor;

        impl<'de> de::Visitor<'de> for UpsertsVisitor {
            type Value = Upserts;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Upserts, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut out = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    if out.insert(key.clone(), value).is_some() {
                        return Err(de::Error::custom(format!(
                            "duplicate key in upserts: {key:?}"
                        )));
                    }
                }
                Ok(Upserts(out))
            }
        }

        deserializer.deserialize_map(UpsertsVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitRequest {
    /// `null` for the first commit of a dataset, otherwise the version the
    /// commit is based on.
    base: Option<u64>,
    request_id: String,
    upserts: Upserts,
    deletes: Vec<String>,
}

/// Canonical commit content used for idempotency comparison: delete order is
/// irrelevant, so deletes are stored as a set.
#[derive(Debug, PartialEq)]
struct CommitContent {
    base: Option<u64>,
    upserts: BTreeMap<String, Value>,
    deletes: BTreeSet<String>,
}

impl CommitContent {
    fn from_request(req: &CommitRequest) -> Self {
        Self {
            base: req.base,
            upserts: req.upserts.0.clone(),
            deletes: req.deletes.iter().cloned().collect(),
        }
    }
}

#[derive(Serialize, Clone)]
struct CommitResponse {
    dataset: String,
    version: u64,
    request_id: String,
}

// ---------------------------------------------------------------------------
// Snapshot response model
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Record {
    key: String,
    value: Value,
}

#[derive(Serialize)]
struct SnapshotResponse {
    dataset: String,
    version: u64,
    records: Vec<Record>,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Dataset {
    current_version: u64,
    /// Records as of `current_version`.
    current: BTreeMap<String, Value>,
    /// Immutable snapshots; `snapshots[v - 1]` is version `v`.
    snapshots: Vec<BTreeMap<String, Value>>,
    /// Idempotency ledger: request_id -> (content, response that was returned).
    requests: HashMap<String, (CommitContent, CommitResponse)>,
}

#[derive(Default)]
struct Store {
    datasets: HashMap<String, Dataset>,
}

type SharedStore = Arc<Mutex<Store>>;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn commit_dataset(
    State(store): State<SharedStore>,
    Path(name): Path<String>,
    body: Result<Json<CommitRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CommitResponse>), ApiError> {
    if name.trim().is_empty() {
        return Err(ApiError::bad_request(
            "empty_name",
            "dataset name must not be empty",
        ));
    }

    let Json(req) = body.map_err(|rejection| {
        let message = rejection.to_string();
        let code = if message.contains("duplicate key in upserts") {
            "duplicate_key"
        } else {
            "invalid_request"
        };
        ApiError::bad_request(code, format!("invalid commit request body: {message}"))
    })?;

    if req.request_id.is_empty() {
        return Err(ApiError::bad_request(
            "empty_request_id",
            "request_id must not be empty",
        ));
    }
    if req.upserts.0.is_empty() && req.deletes.is_empty() {
        return Err(ApiError::bad_request(
            "empty_change",
            "commit must contain at least one upsert or delete",
        ));
    }
    for key in req.upserts.0.keys().chain(req.deletes.iter()) {
        if key.is_empty() {
            return Err(ApiError::bad_request(
                "empty_key",
                "keys must not be empty",
            ));
        }
    }
    let mut seen = BTreeSet::new();
    for key in &req.deletes {
        if !seen.insert(key) {
            return Err(ApiError::bad_request(
                "duplicate_key",
                format!("duplicate key in deletes: {key:?}"),
            ));
        }
    }
    for key in req.deletes.iter() {
        if req.upserts.0.contains_key(key) {
            return Err(ApiError::bad_request(
                "duplicate_key",
                format!("key {key:?} appears in both upserts and deletes"),
            ));
        }
    }

    let content = CommitContent::from_request(&req);

    // The whole check-and-apply runs under one lock, so a commit is atomic:
    // concurrent commits on the same base cannot both succeed, and readers
    // only ever observe the state before or after a complete commit.
    let mut store = store.lock().expect("store lock poisoned");

    // Idempotency takes precedence over the base check: a replayed
    // request_id with identical content returns the original response even
    // if its base is now stale.
    if let Some(dataset) = store.datasets.get(&name)
        && let Some((seen_content, seen_response)) = dataset.requests.get(&req.request_id)
    {
        if *seen_content == content {
            return Ok((StatusCode::CREATED, Json(seen_response.clone())));
        }
        return Err(ApiError::conflict(
            "request_id_conflict",
            format!(
                "request_id {:?} was already used with different content",
                req.request_id
            ),
        ));
    }

    let current_version = store
        .datasets
        .get(&name)
        .map(|d| d.current_version)
        .unwrap_or(0);
    let base_ok = match req.base {
        None => current_version == 0,
        Some(base) => current_version >= 1 && base == current_version,
    };
    if !base_ok {
        return Err(ApiError::conflict(
            "stale_base",
            format!(
                "base {:?} does not match current version {current_version}",
                req.base
            ),
        ));
    }

    let dataset = store.datasets.entry(name.clone()).or_default();
    let mut records = dataset.current.clone();
    for key in &req.deletes {
        records.remove(key);
    }
    for (key, value) in &req.upserts.0 {
        records.insert(key.clone(), value.clone());
    }

    dataset.current_version += 1;
    dataset.current = records.clone();
    dataset.snapshots.push(records);

    let response = CommitResponse {
        dataset: name,
        version: dataset.current_version,
        request_id: req.request_id.clone(),
    };
    dataset
        .requests
        .insert(req.request_id.clone(), (content, response.clone()));

    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_snapshot(
    State(store): State<SharedStore>,
    Path((name, version)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    let store = store.lock().expect("store lock poisoned");

    let dataset = store
        .datasets
        .get(&name)
        .ok_or_else(|| ApiError::not_found(format!("unknown dataset: {name:?}")))?;

    let version = if version == "latest" {
        dataset.current_version
    } else {
        version
            .parse::<u64>()
            .ok()
            .filter(|v| *v >= 1)
            .ok_or_else(|| ApiError::not_found(format!("unknown version: {version:?}")))?
    };

    let snapshot = dataset
        .snapshots
        .get((version - 1) as usize)
        .ok_or_else(|| {
            ApiError::not_found(format!("dataset {name:?} has no version {version}"))
        })?;

    // BTreeMap<String, _> iterates in byte order, which for UTF-8 is the
    // same as Unicode code point order.
    let records = snapshot
        .iter()
        .map(|(key, value)| Record {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();

    Ok(Json(SnapshotResponse {
        dataset: name,
        version,
        records,
    }))
}

async fn fallback() -> ApiError {
    ApiError::not_found("route not found")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let store: SharedStore = Arc::new(Mutex::new(Store::default()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/{name}/commits", post(commit_dataset))
        .route("/datasets/{name}/snapshots/{version}", get(get_snapshot))
        .fallback(fallback)
        .with_state(store);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
