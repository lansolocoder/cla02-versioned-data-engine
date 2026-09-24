use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    sync::{Arc, RwLock},
};

type Fields = Map<String, Value>;

/// In-memory dataset and its saved snapshots.
///
/// `records` keeps the live dataset; `snapshots[i]` is the immutable
/// snapshot of version `i + 1`. One lock guards both, so a batch write and
/// a snapshot can never interleave: every snapshot is either the state
/// before or after a complete batch, never a half-applied one.
#[derive(Default)]
struct Store {
    records: BTreeMap<String, Fields>,
    snapshots: Vec<BTreeMap<String, Fields>>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<Store>>,
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

#[derive(Serialize)]
struct WriteResponse {
    written: usize,
    total: usize,
}

#[derive(Serialize)]
struct SaveResponse {
    version: usize,
    total: usize,
}

#[derive(Serialize)]
struct RecordResponse {
    key: String,
    fields: Value,
}

#[derive(Serialize)]
struct SnapshotResponse {
    version: usize,
    records: Vec<RecordResponse>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    /// 0-based index of the offending record within the batch, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
}

impl ErrorResponse {
    fn new(msg: impl Into<String>) -> Self {
        ErrorResponse {
            error: msg.into(),
            index: None,
        }
    }

    fn at(index: usize, msg: impl Into<String>) -> Self {
        ErrorResponse {
            error: msg.into(),
            index: Some(index),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
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

/// Validate one record of the batch. Returns the owned key and fields.
fn parse_record(index: usize, value: &Value) -> Result<(String, Fields), ErrorResponse> {
    let object = value
        .as_object()
        .ok_or_else(|| ErrorResponse::at(index, "record must be a JSON object"))?;

    let key = object
        .get("key")
        .ok_or_else(|| ErrorResponse::at(index, "record is missing field `key`"))?
        .as_str()
        .ok_or_else(|| ErrorResponse::at(index, "field `key` must be a string"))?
        .to_owned();

    let fields = match object.get("fields") {
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(ErrorResponse::at(
                index,
                "field `fields` must be a JSON object",
            ));
        }
        None => return Err(ErrorResponse::at(index, "record is missing field `fields`")),
    };

    Ok((key, fields))
}

/// `POST /records` — apply one batch of records, all or nothing.
async fn write_records(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Json<WriteResponse>, ErrorResponse> {
    let body: Value = serde_json::from_slice(&body)
        .map_err(|err| ErrorResponse::new(format!("request body must be JSON: {err}")))?;
    let batch = body
        .as_array()
        .ok_or_else(|| ErrorResponse::new("request body must be a JSON array of records"))?;

    if batch.is_empty() {
        return Err(ErrorResponse::new("batch must not be empty"));
    }

    // Validate the whole batch before touching the dataset, so any failure
    // leaves the store exactly as it was before the request.
    let mut parsed: Vec<(String, Fields)> = Vec::with_capacity(batch.len());
    for (index, item) in batch.iter().enumerate() {
        let (key, fields) = parse_record(index, item)?;
        if parsed.iter().any(|(existing, _)| existing == &key) {
            return Err(ErrorResponse::at(
                index,
                "duplicate key within the same batch",
            ));
        }
        parsed.push((key, fields));
    }

    let written = parsed.len();
    let mut store = state.store.write().expect("store lock poisoned");
    for (key, fields) in parsed {
        // An existing key is replaced wholesale by the new fields.
        store.records.insert(key, fields);
    }
    let total = store.records.len();

    Ok(Json(WriteResponse { written, total }))
}

/// `POST /versions` — freeze the current dataset as an immutable snapshot.
async fn save_version(State(state): State<AppState>) -> Json<SaveResponse> {
    let mut store = state.store.write().expect("store lock poisoned");
    // Clone while holding the lock so the snapshot captures the complete
    // dataset of one instant; writes afterwards mutate a different map.
    let snapshot = store.records.clone();
    let total = snapshot.len();
    store.snapshots.push(snapshot);
    Json(SaveResponse {
        version: store.snapshots.len(),
        total,
    })
}

/// Percent-decode an `application/x-www-form-urlencoded` value.
fn url_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = |b: u8| match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                };
                if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    out.push(hi * 16 + lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Pull `key` out of the raw query string without the axum `query` feature.
fn query_key(uri: &axum::http::Uri) -> Option<String> {
    let query = uri.query()?;
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("key=") {
            return Some(url_decode(value));
        }
    }
    None
}

/// `GET /versions/{version}` (optional `?key=...`) — read a saved snapshot.
async fn get_snapshot(
    State(state): State<AppState>,
    axum::extract::Path(raw): axum::extract::Path<String>,
    uri: axum::http::Uri,
) -> Result<Json<SnapshotResponse>, ErrorResponse> {
    let version: usize = raw
        .parse()
        .map_err(|_| ErrorResponse::new(format!("`{raw}` is not a valid version number")))?;
    let store = state.store.read().expect("store lock poisoned");
    let snapshot = store
        .snapshots
        .get(
            version
                .checked_sub(1)
                .ok_or_else(|| ErrorResponse::new("version must be a positive integer"))?,
        )
        .ok_or_else(|| ErrorResponse::new(format!("version {version} does not exist")))?;

    let records = if let Some(key) = query_key(&uri) {
        let fields = snapshot.get(&key).ok_or_else(|| {
            ErrorResponse::new(format!("key `{key}` not found in version {version}"))
        })?;
        vec![RecordResponse {
            key,
            fields: Value::Object(fields.clone()),
        }]
    } else {
        // BTreeMap iterates in key lexicographic order, giving stable output.
        snapshot
            .iter()
            .map(|(key, fields)| RecordResponse {
                key: key.clone(),
                fields: Value::Object(fields.clone()),
            })
            .collect()
    };

    Ok(Json(SnapshotResponse { version, records }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState {
        store: Arc::new(RwLock::new(Store::default())),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/records", post(write_records))
        .route("/versions", post(save_version))
        .route("/versions/{version}", get(get_snapshot))
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
