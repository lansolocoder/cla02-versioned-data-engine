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

/// Key-only filter for record listing, parsed from the query string.
/// Either a prefix match, or a lower/upper bound range (each bound
/// inclusive or exclusive), but never both.
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
            return key.starts_with(prefix.as_str());
        }
        if let Some((bound, inclusive)) = &self.lower {
            let ok = if *inclusive {
                key >= bound.as_str()
            } else {
                key > bound.as_str()
            };
            if !ok {
                return false;
            }
        }
        if let Some((bound, inclusive)) = &self.upper {
            let ok = if *inclusive {
                key <= bound.as_str()
            } else {
                key < bound.as_str()
            };
            if !ok {
                return false;
            }
        }
        true
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decode one query-string component: `%XX` escapes and `+` as space.
fn percent_decode(s: &str) -> Result<String, ()> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(());
                }
                let hi = hex_val(bytes[i + 1]).ok_or(())?;
                let lo = hex_val(bytes[i + 2]).ok_or(())?;
                out.push(hi << 4 | lo);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

/// Parse the query string into a key filter. Any parameter outside the
/// documented set (`prefix`, `gte`, `gt`, `lte`, `lt`), duplicates,
/// conflicting bounds, or values that are not valid key fragments
/// (non-empty, `[A-Za-z0-9_-]`) are rejected.
fn parse_key_filter(query: &str) -> Result<KeyFilter, ()> {
    let mut filter = KeyFilter::default();
    if query.is_empty() {
        return Ok(filter);
    }
    let mut gte: Option<String> = None;
    let mut gt: Option<String> = None;
    let mut lte: Option<String> = None;
    let mut lt: Option<String> = None;

    for pair in query.split('&') {
        let (raw_name, raw_value) = pair.split_once('=').ok_or(())?;
        let name = percent_decode(raw_name)?;
        let value = percent_decode(raw_value)?;
        if !valid_identifier(&value) {
            return Err(());
        }
        let slot = match name.as_str() {
            "prefix" => &mut filter.prefix,
            "gte" => &mut gte,
            "gt" => &mut gt,
            "lte" => &mut lte,
            "lt" => &mut lt,
            _ => return Err(()),
        };
        if slot.is_some() {
            return Err(());
        }
        *slot = Some(value);
    }

    if filter.prefix.is_some() && (gte.is_some() || gt.is_some() || lte.is_some() || lt.is_some())
    {
        return Err(());
    }
    if gte.is_some() && gt.is_some() {
        return Err(());
    }
    if lte.is_some() && lt.is_some() {
        return Err(());
    }
    filter.lower = gte.map(|b| (b, true)).or_else(|| gt.map(|b| (b, false)));
    filter.upper = lte.map(|b| (b, true)).or_else(|| lt.map(|b| (b, false)));
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
    let filter = match parse_key_filter(uri.query().unwrap_or("")) {
        Ok(filter) => filter,
        Err(()) => return error_response(StatusCode::BAD_REQUEST, "invalid_query"),
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
        // Filter on the key alone, before reading: records outside the hit
        // set are excluded even if their file is corrupt or unreadable.
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
