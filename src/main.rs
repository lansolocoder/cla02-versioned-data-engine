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
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
