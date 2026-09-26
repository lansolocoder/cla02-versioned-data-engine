use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
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

type Records = BTreeMap<String, Map<String, Value>>;

#[derive(Default)]
struct Collection {
    records: Records,
    snapshots: Vec<Records>,
}

type Store = Arc<Mutex<HashMap<String, Collection>>>;

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        Json(ErrorBody {
            error: ErrorDetail { code },
        }),
    )
        .into_response()
}

fn invalid_records() -> Response {
    error_response(StatusCode::BAD_REQUEST, "invalid_records")
}

fn snapshot_not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "snapshot_not_found")
}

async fn put_records(
    State(store): State<Store>,
    Path(collection): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let Some(records) = body.get("records").and_then(Value::as_array) else {
        return invalid_records();
    };

    let mut batch: Vec<(String, Map<String, Value>)> = Vec::with_capacity(records.len());
    let mut seen: HashSet<&str> = HashSet::with_capacity(records.len());
    for item in records {
        let (Some(id), Some(data)) = (
            item.get("id").and_then(Value::as_str),
            item.get("data").and_then(Value::as_object),
        ) else {
            return invalid_records();
        };
        if id.is_empty() || !seen.insert(id) {
            return invalid_records();
        }
        batch.push((id.to_owned(), data.clone()));
    }

    let mut store = store.lock().unwrap();
    let collection = store.entry(collection).or_default();
    for (id, data) in &batch {
        collection.records.insert(id.clone(), data.clone());
    }
    Json(serde_json::json!({ "applied": batch.len() })).into_response()
}

async fn post_snapshot(State(store): State<Store>, Path(collection): Path<String>) -> Response {
    let mut store = store.lock().unwrap();
    let collection = store.entry(collection).or_default();
    collection.snapshots.push(collection.records.clone());
    Json(serde_json::json!({ "version": collection.snapshots.len() })).into_response()
}

async fn list_snapshots(State(store): State<Store>, Path(collection): Path<String>) -> Response {
    let store = store.lock().unwrap();
    let versions: Vec<Value> = store
        .get(&collection)
        .map(|c| {
            c.snapshots
                .iter()
                .enumerate()
                .map(|(i, records)| {
                    serde_json::json!({ "version": i + 1, "recordCount": records.len() })
                })
                .collect()
        })
        .unwrap_or_default();
    Json(serde_json::json!({ "versions": versions })).into_response()
}

fn snapshot_records(collection: &Collection, version: u64) -> Option<&Records> {
    if version == 0 {
        return None;
    }
    collection.snapshots.get((version - 1) as usize)
}

fn records_json(records: &Records) -> Vec<Value> {
    records
        .iter()
        .map(|(id, data)| serde_json::json!({ "id": id, "data": data }))
        .collect()
}

async fn get_snapshot(
    State(store): State<Store>,
    Path((collection, version)): Path<(String, u64)>,
) -> Response {
    let store = store.lock().unwrap();
    let Some(records) = store
        .get(&collection)
        .and_then(|c| snapshot_records(c, version))
    else {
        return snapshot_not_found();
    };
    Json(serde_json::json!({ "version": version, "records": records_json(records) }))
        .into_response()
}

async fn diff_snapshots(
    State(store): State<Store>,
    Path((collection, from, to)): Path<(String, u64, u64)>,
) -> Response {
    if from > to {
        return error_response(StatusCode::BAD_REQUEST, "invalid_snapshot_range");
    }
    let store = store.lock().unwrap();
    let Some(collection) = store.get(&collection) else {
        return snapshot_not_found();
    };
    let (Some(from_records), Some(to_records)) = (
        snapshot_records(collection, from),
        snapshot_records(collection, to),
    ) else {
        return snapshot_not_found();
    };

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for id in from_records.keys() {
        match to_records.get(id) {
            None => removed.push(id.clone()),
            Some(data) if *data != from_records[id] => changed.push(id.clone()),
            _ => {}
        }
    }
    for id in to_records.keys() {
        if !from_records.contains_key(id) {
            added.push(id.clone());
        }
    }

    Json(serde_json::json!({
        "from": from,
        "to": to,
        "added": added,
        "removed": removed,
        "changed": changed,
    }))
    .into_response()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/collections/{collection}/records", put(put_records))
        .route(
            "/collections/{collection}/snapshots",
            get(list_snapshots).post(post_snapshot),
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
