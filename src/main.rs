use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    error::Error,
    sync::{Arc, RwLock},
};

/// Immutable frozen copy of a collection's records, keyed by id.
type Snapshot = BTreeMap<String, Value>;

struct Collection {
    /// Live records, overwritten by successful batches.
    records: BTreeMap<String, Value>,
    /// Frozen snapshots indexed by their 1-based version.
    snapshots: Vec<Snapshot>,
}

impl Collection {
    fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            snapshots: Vec::new(),
        }
    }
}

#[derive(Default)]
struct Store {
    /// Collections appear on the first successful write; empty string ids are
    /// rejected so a collection is never created for an invalid batch.
    collections: HashMap<String, Collection>,
}

type SharedStore = Arc<RwLock<Store>>;

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
struct AppliedBody {
    applied: usize,
}

#[derive(Serialize)]
struct SnapshotBody {
    version: usize,
}

#[derive(Serialize)]
struct VersionInfo {
    version: usize,
    #[serde(rename = "recordCount")]
    record_count: usize,
}

#[derive(Serialize)]
struct SnapshotListBody {
    versions: Vec<VersionInfo>,
}

#[derive(Serialize)]
struct RecordOut {
    id: String,
    data: Value,
}

#[derive(Serialize)]
struct SnapshotRecordsBody {
    version: usize,
    records: Vec<RecordOut>,
}

#[derive(Serialize)]
struct DiffBody {
    from: usize,
    to: usize,
    added: Vec<String>,
    removed: Vec<String>,
    changed: Vec<String>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
}

enum ApiError {
    InvalidRecords,
    SnapshotNotFound,
    InvalidSnapshotRange,
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            ApiError::InvalidRecords => "invalid_records",
            ApiError::SnapshotNotFound => "snapshot_not_found",
            ApiError::InvalidSnapshotRange => "invalid_snapshot_range",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            ApiError::InvalidRecords | ApiError::InvalidSnapshotRange => {
                StatusCode::BAD_REQUEST
            }
            ApiError::SnapshotNotFound => StatusCode::NOT_FOUND,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status(),
            Json(ErrorBody {
                error: ErrorDetail { code: self.code() },
            }),
        )
            .into_response()
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

async fn put_records(
    State(store): State<SharedStore>,
    Path(collection): Path<String>,
    body: Bytes,
) -> Result<Json<AppliedBody>, ApiError> {
    // Parse manually so malformed JSON and wrong types yield the spec's
    // invalid_records 400 rather than the extractor's generic 400/422.
    let mut payload: Value = serde_json::from_slice(&body).map_err(|_| ApiError::InvalidRecords)?;
    let records = match payload
        .get_mut("records")
        .and_then(Value::as_array_mut)
        .map(std::mem::take)
    {
        Some(records) => records,
        None => return Err(ApiError::InvalidRecords),
    };

    // Validate the whole batch before touching anything: missing id, empty
    // id, duplicate id within the batch, or non-object data rejects all.
    let mut validated: Vec<(String, Value)> = Vec::with_capacity(records.len());
    let mut seen = HashSet::with_capacity(records.len());
    for record in records {
        let mut record = match record {
            Value::Object(map) => map,
            _ => return Err(ApiError::InvalidRecords),
        };
        let id = match record.remove("id") {
            Some(Value::String(id)) if !id.is_empty() => id,
            _ => return Err(ApiError::InvalidRecords),
        };
        let data = match record.remove("data") {
            Some(data @ Value::Object(_)) => data,
            _ => return Err(ApiError::InvalidRecords),
        };
        if !seen.insert(id.clone()) {
            return Err(ApiError::InvalidRecords);
        }
        validated.push((id, data));
    }

    let applied = validated.len();
    let mut guard = store.write().unwrap();
    let entry = guard
        .collections
        .entry(collection)
        .or_insert_with(Collection::new);
    for (id, data) in validated {
        entry.records.insert(id, data);
    }
    Ok(Json(AppliedBody { applied }))
}

async fn post_snapshot(
    State(store): State<SharedStore>,
    Path(collection): Path<String>,
) -> Json<SnapshotBody> {
    let mut guard = store.write().unwrap();
    let entry = guard
        .collections
        .entry(collection)
        .or_insert_with(Collection::new);
    // Snapshot of empty collection is still a successful freeze at V=1; it
    // simply contains no records.
    entry.snapshots.push(entry.records.clone());
    Json(SnapshotBody {
        version: entry.snapshots.len(),
    })
}

async fn list_snapshots(
    State(store): State<SharedStore>,
    Path(collection): Path<String>,
) -> Json<SnapshotListBody> {
    let guard = store.read().unwrap();
    let versions = match guard.collections.get(&collection) {
        Some(c) => c
            .snapshots
            .iter()
            .enumerate()
            .map(|(i, s)| VersionInfo {
                version: i + 1,
                record_count: s.len(),
            })
            .collect(),
        None => Vec::new(),
    };
    Json(SnapshotListBody { versions })
}

async fn get_snapshot(
    State(store): State<SharedStore>,
    Path((collection, version)): Path<(String, usize)>,
) -> Result<Json<SnapshotRecordsBody>, ApiError> {
    let guard = store.read().unwrap();
    let collection = guard
        .collections
        .get(&collection)
        .ok_or(ApiError::SnapshotNotFound)?;
    // Versions are 1-based; index 0 or out of range does not exist.
    let snapshot = collection
        .snapshots
        .get(version.checked_sub(1).ok_or(ApiError::SnapshotNotFound)?)
        .ok_or(ApiError::SnapshotNotFound)?;
    let records = snapshot
        .iter()
        .map(|(id, data)| RecordOut {
            id: id.clone(),
            data: data.clone(),
        })
        .collect();
    Ok(Json(SnapshotRecordsBody {
        version,
        records,
    }))
}

async fn diff_snapshots(
    State(store): State<SharedStore>,
    Path((collection, from, to)): Path<(String, usize, usize)>,
) -> Result<Json<DiffBody>, ApiError> {
    if from > to {
        return Err(ApiError::InvalidSnapshotRange);
    }
    let guard = store.read().unwrap();
    let collection = guard
        .collections
        .get(&collection)
        .ok_or(ApiError::SnapshotNotFound)?;
    let index = |v: usize| -> Result<&Snapshot, ApiError> {
        collection
            .snapshots
            .get(v.checked_sub(1).ok_or(ApiError::SnapshotNotFound)?)
            .ok_or(ApiError::SnapshotNotFound)
    };
    let from_snapshot = index(from)?;
    let to_snapshot = index(to)?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (id, to_data) in to_snapshot {
        match from_snapshot.get(id) {
            None => added.push(id.clone()),
            Some(from_data) if from_data != to_data => changed.push(id.clone()),
            _ => {}
        }
    }
    for id in from_snapshot.keys() {
        if !to_snapshot.contains_key(id) {
            removed.push(id.clone());
        }
    }
    // BTreeMap iteration is already id-ascending; sort anyway to make the
    // contract explicit and independent of the underlying map.
    added.sort();
    removed.sort();
    changed.sort();

    Ok(Json(DiffBody {
        from,
        to,
        added,
        removed,
        changed,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let store = Arc::new(RwLock::new(Store::default()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route(
            "/collections/{collection}/records",
            put(put_records),
        )
        .route(
            "/collections/{collection}/snapshots",
            post(post_snapshot).get(list_snapshots),
        )
        .route(
            "/collections/{collection}/snapshots/{version}",
            get(get_snapshot),
        )
        .route(
            "/collections/{collection}/snapshots/{from}/diff/{to}",
            get(diff_snapshots),
        )
        .with_state(store);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
