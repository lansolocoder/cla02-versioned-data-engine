use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    env,
    error::Error,
    io::{ErrorKind, Write},
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

struct AppState {
    data_dir: PathBuf,
    tmp_seq: AtomicU64,
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

fn error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn is_valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Split a wildcard path tail into (dataset, key) for `{name}/records/{key}`.
fn parse_segments(rest: &str) -> Option<(&str, &str)> {
    let mut parts = rest.split('/');
    let name = parts.next()?;
    if parts.next()? != "records" {
        return None;
    }
    let key = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some((name, key))
}

fn record_path(state: &AppState, name: &str, key: &str) -> PathBuf {
    state.data_dir.join(name).join(format!("{key}.json"))
}

/// Atomically persist `bytes` to `file`: write temp file, fsync, rename, fsync dir.
fn write_atomic(dir: PathBuf, tmp: PathBuf, file: PathBuf, bytes: Vec<u8>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dir)?;
    let write_result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()
    })();
    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&tmp, &file) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::File::open(&dir)?.sync_all()?;
    Ok(())
}

async fn put_record(
    State(state): State<Arc<AppState>>,
    Path(rest): Path<String>,
    body: Bytes,
) -> Response {
    let Some((name, key)) = parse_segments(&rest) else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    if !is_valid_ident(name) || !is_valid_ident(key) {
        return error(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_json"),
    };

    let dir = state.data_dir.join(name);
    let file = record_path(&state, name, key);
    let seq = state.tmp_seq.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{key}.tmp-{}-{seq}", std::process::id()));
    let bytes = body.to_vec();

    let result = tokio::task::spawn_blocking(move || write_atomic(dir, tmp, file, bytes)).await;
    match result {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({ "key": key, "value": value }))).into_response(),
        _ => error(StatusCode::INTERNAL_SERVER_ERROR, "io_error"),
    }
}

async fn get_record(State(state): State<Arc<AppState>>, Path(rest): Path<String>) -> Response {
    let Some((name, key)) = parse_segments(&rest) else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    if !is_valid_ident(name) || !is_valid_ident(key) {
        return error(StatusCode::BAD_REQUEST, "invalid_identifier");
    }

    let file = record_path(&state, name, key);
    let result = tokio::task::spawn_blocking(move || std::fs::read(&file)).await;
    match result {
        Ok(Ok(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => {
                (StatusCode::OK, Json(json!({ "key": key, "value": value }))).into_response()
            }
            Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "io_error"),
        },
        Ok(Err(e)) if e.kind() == ErrorKind::NotFound => error(StatusCode::NOT_FOUND, "not_found"),
        _ => error(StatusCode::INTERNAL_SERVER_ERROR, "io_error"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let data_dir = env::var("VDE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));
    std::fs::create_dir_all(&data_dir)?;

    let state = Arc::new(AppState {
        data_dir,
        tmp_seq: AtomicU64::new(0),
    });

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/{*rest}", put(put_record).get(get_record))
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
