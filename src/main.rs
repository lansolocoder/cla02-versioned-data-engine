use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    env,
    error::Error,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
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

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn record_path(state: &AppState, name: &str, key: &str) -> PathBuf {
    state.data_dir.join(name).join(format!("{key}.json"))
}

async fn put_record(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if !valid_identifier(&name) || !valid_identifier(&key) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    if body.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "invalid_json");
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_json"),
    };

    if let Err(resp) = persist_record(&state, &name, &key, &body).await {
        return resp;
    }
    Json(json!({ "key": key, "value": value })).into_response()
}

/// Write the record atomically: temp file + fsync + rename + dir fsync, so a
/// crash either leaves the old value intact or the new one fully in place.
async fn persist_record(
    state: &AppState,
    name: &str,
    key: &str,
    body: &[u8],
) -> Result<(), Response> {
    let dir = state.data_dir.join(name);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        tracing_error(&e);
        return Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ));
    }

    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let tmp = dir.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));

    let result = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, body).await?;
        file.sync_all().await?;
        tokio::fs::rename(&tmp, record_path(state, name, key)).await?;
        // Fsync the directory so the rename itself is durable.
        let dir_file = tokio::fs::File::open(&dir).await?;
        dir_file.sync_all().await?;
        Ok::<(), std::io::Error>(())
    }
    .await;

    if let Err(e) = result {
        let _ = tokio::fs::remove_file(&tmp).await;
        tracing_error(&e);
        return Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ));
    }
    Ok(())
}

fn tracing_error(e: &std::io::Error) {
    eprintln!("storage error: {e}");
}

async fn get_record(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> Response {
    if !valid_identifier(&name) || !valid_identifier(&key) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }

    let bytes = match tokio::fs::read(record_path(&state, &name, &key)).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return error_response(StatusCode::NOT_FOUND, "not_found");
        }
        Err(e) => {
            tracing_error(&e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => Json(json!({ "key": key, "value": value })).into_response(),
        Err(e) => {
            eprintln!("corrupt record {name}/{key}: {e}");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal")
        }
    }
}

/// Format a timestamp as RFC3339 UTC with second precision, e.g.
/// `2006-01-02T15:04:05Z`. Civil-from-days conversion per Howard Hinnant.
fn format_rfc3339_utc(t: SystemTime) -> String {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86400) as i64;
    let sod = secs % 86400;
    let (hour, min, sec) = (sod / 3600, (sod % 3600) / 60, sod % 60);

    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Key filter parsed from the list-records query string. All present
/// conditions must hold for a key to match (logical AND).
#[derive(Default)]
struct KeyFilter {
    prefix: Option<String>,
    /// (bound, inclusive)
    lower: Option<(String, bool)>,
    /// (bound, inclusive)
    upper: Option<(String, bool)>,
}

impl KeyFilter {
    fn matches(&self, key: &str) -> bool {
        if let Some(prefix) = &self.prefix {
            if !key.starts_with(prefix.as_str()) {
                return false;
            }
        }
        if let Some((bound, inclusive)) = &self.lower {
            let ord = key.cmp(bound);
            if ord == std::cmp::Ordering::Less || (!inclusive && ord == std::cmp::Ordering::Equal) {
                return false;
            }
        }
        if let Some((bound, inclusive)) = &self.upper {
            let ord = key.cmp(bound);
            if ord == std::cmp::Ordering::Greater || (!inclusive && ord == std::cmp::Ordering::Equal)
            {
                return false;
            }
        }
        true
    }
}

/// Parse the query string of `GET /datasets/{name}/records`. Only the
/// documented filter parameters are allowed; anything else, duplicates,
/// conflicting bounds, or malformed values yield 400 `invalid_query`.
fn parse_key_filter(query: Option<&str>) -> Result<KeyFilter, Response> {
    let invalid = || error_response(StatusCode::BAD_REQUEST, "invalid_query");
    let mut filter = KeyFilter::default();
    let query = match query {
        None => return Ok(filter),
        Some(q) if q.is_empty() => return Ok(filter),
        Some(q) => q,
    };
    for pair in query.split('&') {
        let (param, value) = pair.split_once('=').ok_or_else(invalid)?;
        // Filter values use the same charset as keys; anything else
        // (including empty values) cannot be parsed into a key bound.
        if !valid_identifier(value) {
            return Err(invalid());
        }
        let value = value.to_owned();
        match param {
            "prefix" => {
                if filter.prefix.replace(value).is_some() {
                    return Err(invalid());
                }
            }
            "gte" | "gt" => {
                if filter.lower.is_some() {
                    return Err(invalid());
                }
                filter.lower = Some((value, param == "gte"));
            }
            "lte" | "le" => {
                if filter.upper.is_some() {
                    return Err(invalid());
                }
                filter.upper = Some((value, param == "lte"));
            }
            _ => return Err(invalid()),
        }
    }
    Ok(filter)
}

async fn list_records(
    State(state): State<AppState>,
    Path(name): Path<String>,
    uri: Uri,
) -> Response {
    if !valid_identifier(&name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    let filter = match parse_key_filter(uri.query()) {
        Ok(filter) => filter,
        Err(resp) => return resp,
    };

    let dir = state.data_dir.join(&name);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return error_response(StatusCode::NOT_FOUND, "not_found");
        }
        Err(e) => {
            tracing_error(&e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };

    let mut records: Vec<(String, Value, SystemTime)> = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) => {
                tracing_error(&e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(key) = file_name.strip_suffix(".json") else {
            continue;
        };
        // Filter on the key alone, before reading: records outside the
        // matching set are never touched, so a corrupt file out of range
        // cannot fail this request.
        if !filter.matches(key) {
            continue;
        }

        let result = async {
            let bytes = tokio::fs::read(entry.path()).await?;
            let modified = entry.metadata().await?.modified()?;
            Ok::<(Vec<u8>, SystemTime), std::io::Error>((bytes, modified))
        }
        .await;
        let (bytes, modified) = match result {
            Ok(ok) => ok,
            Err(e) => {
                tracing_error(&e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(e) => {
                eprintln!("corrupt record {name}/{key}: {e}");
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        records.push((key.to_owned(), value, modified));
    }

    // Byte-wise order on UTF-8 is Unicode code-point order.
    records.sort_by(|a, b| a.0.cmp(&b.0));
    let records: Vec<Value> = records
        .into_iter()
        .map(|(key, value, modified)| {
            json!({
                "key": key,
                "value": value,
                "updatedAt": format_rfc3339_utc(modified),
            })
        })
        .collect();
    Json(json!({ "name": name, "records": records })).into_response()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let data_dir = env::var("VDE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));
    let state = AppState {
        data_dir: Arc::new(data_dir),
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route(
            "/datasets/{name}/records/{key}",
            put(put_record).get(get_record),
        )
        .route("/datasets/{name}/records", get(list_records))
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
