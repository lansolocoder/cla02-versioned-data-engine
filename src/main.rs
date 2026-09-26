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

/// Convert a `SystemTime` to a UTC RFC3339 timestamp with second precision,
/// e.g. `2006-01-02T15:04:05Z`. Returns `None` for times before the Unix
/// epoch or outside the representable range.
fn system_time_to_rfc3339(t: std::time::SystemTime) -> Option<String> {
    let secs = t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
    Some(unix_seconds_to_rfc3339(secs))
}

/// Days since the Unix epoch to a proleptic Gregorian `(year, month, day)`,
/// using Howard Hinnant's civil-from-days algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

fn unix_seconds_to_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

async fn list_records(
    State(state): State<AppState>,
    Path(name): Path<String>,
    uri: Uri,
) -> Response {
    if !valid_identifier(&name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    if uri.query().is_some() {
        return error_response(StatusCode::BAD_REQUEST, "invalid_query");
    }

    let dir = state.data_dir.join(&name);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return error_response(StatusCode::NOT_FOUND, "not_found");
        }
        Err(e) => {
            tracing_error(&e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };

    // Read every record fully before producing any output, so a corrupt or
    // unreadable file fails the whole request with 500 instead of a partial
    // list.
    let mut records: Vec<(String, Value, String)> = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                tracing_error(&e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(key) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        // Only surface files the PUT route could have written.
        if !valid_identifier(key) {
            continue;
        }

        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) => {
                tracing_error(&e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("corrupt record {name}/{key}: {e}");
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        let updated_at = match entry
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(system_time_to_rfc3339)
        {
            Some(ts) => ts,
            None => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
            }
        };
        records.push((key.to_owned(), value, updated_at));
    }

    // UTF-8 byte order equals Unicode code point order; record keys are
    // restricted to ASCII identifier characters.
    records.sort_by(|a, b| a.0.cmp(&b.0));

    let items: Vec<Value> = records
        .into_iter()
        .map(|(key, value, updated_at)| json!({ "key": key, "value": value, "updatedAt": updated_at }))
        .collect();
    Json(json!({ "name": name, "records": items })).into_response()
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
