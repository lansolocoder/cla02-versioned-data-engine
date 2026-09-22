mod engine;

use axum::{
    Json, Router,
    body::Bytes,
    http::StatusCode,
    routing::{get, post},
};
use serde::Serialize;
use serde_json::Value;
use std::{env, error::Error};

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
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
    path: String,
}

type ApiResult = Result<Json<Value>, (StatusCode, Json<ErrorBody>)>;

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn version() -> Json<Version> {
    Json(Version {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    })
}

fn error_response(err: engine::ApiError) -> (StatusCode, Json<ErrorBody>) {
    let status = StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        Json(ErrorBody {
            error: ErrorDetail {
                code: err.code,
                message: err.message,
                path: err.path,
            },
        }),
    )
}

fn parse_json(body: &Bytes) -> Result<Value, (StatusCode, Json<ErrorBody>)> {
    serde_json::from_slice(body).map_err(|err| {
        error_response(engine::ApiError::bad_request(
            format!("invalid JSON body: {err}"),
            "",
        ))
    })
}

async fn diff(body: Bytes) -> ApiResult {
    let request = parse_json(&body)?;
    engine::diff(&request).map(Json).map_err(error_response)
}

async fn apply(body: Bytes) -> ApiResult {
    let request = parse_json(&body)?;
    engine::apply(&request).map(Json).map_err(error_response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/diff", post(diff))
        .route("/apply", post(apply));

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
