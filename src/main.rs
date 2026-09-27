use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path as PathParam, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};
use tokio::sync::RwLock;

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

/// Unique suffix source for batch staging directories, shared across datasets.
static BATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Per-dataset locks: readers (GET/list) take a shared guard, writers
/// (single PUT and batch PUT) an exclusive one, so a concurrent request can
/// only ever observe the dataset before or after a complete batch.
static DATASET_LOCKS: std::sync::LazyLock<
    RwLock<std::collections::HashMap<String, Arc<RwLock<()>>>>,
> = std::sync::LazyLock::new(|| RwLock::new(std::collections::HashMap::new()));

async fn dataset_lock(name: &str) -> Arc<RwLock<()>> {
    {
        let locks = DATASET_LOCKS.read().await;
        if let Some(lock) = locks.get(name) {
            return lock.clone();
        }
    }
    let mut locks = DATASET_LOCKS.write().await;
    locks
        .entry(name.to_owned())
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
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
    PathParam((name, key)): PathParam<(String, String)>,
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

    // Serialize against any batch in progress on this dataset; single
    // writers serialize with each other on the same lock as well.
    let lock = dataset_lock(&name).await;
    let _guard = lock.write().await;
    if let Err(resp) = write_file_atomic(&state, &name, &key, &body).await {
        return resp;
    }
    Json(json!({ "key": key, "value": value })).into_response()
}

/// Persist one record file in its dataset directory, creating the directory
/// first.
async fn ensure_dataset_dir(state: &AppState, name: &str) -> Result<PathBuf, Response> {
    let dir = state.data_dir.join(name);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        tracing_error(&e);
        return Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ));
    }
    Ok(dir)
}

/// Write `body` to `target` atomically: temp file + fsync + rename + dir
/// fsync, so a crash either leaves the old file intact or the new one fully
/// in place. `dir` must be the directory containing `target`.
async fn write_file_atomic_in(dir: &Path, target: &Path, body: &[u8]) -> std::io::Result<()> {
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
        tokio::fs::rename(&tmp, target).await?;
        // Fsync the directory so the rename itself is durable.
        let dir_file = tokio::fs::File::open(dir).await?;
        dir_file.sync_all().await?;
        Ok::<(), std::io::Error>(())
    }
    .await;

    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Write one record atomically, creating the dataset directory on demand.
async fn write_file_atomic(
    state: &AppState,
    name: &str,
    key: &str,
    body: &[u8],
) -> Result<(), Response> {
    let dir = ensure_dataset_dir(state, name).await?;
    let target = record_path(state, name, key);
    write_file_atomic_in(&dir, &target, body)
        .await
        .map_err(|e| {
            tracing_error(&e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal")
        })
}

fn tracing_error(e: &std::io::Error) {
    eprintln!("storage error: {e}");
}

async fn get_record(
    State(state): State<AppState>,
    PathParam((name, key)): PathParam<(String, String)>,
) -> Response {
    if !valid_identifier(&name) || !valid_identifier(&key) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }

    // Hold a shared guard for the whole read so a batch commit can never
    // expose a half-applied set of files.
    let lock = dataset_lock(&name).await;
    let _guard = lock.read().await;
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
#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Opaque, self-contained continuation token for a paged list. It pins the
/// exact filter the page was issued under and the last key returned, so the
/// next request resumes strictly after that key in code-point order.
/// Keyset positioning (rather than an offset) is what keeps paging stable
/// under concurrent writes: records inserted into already-served positions
/// are never revisited, and deleted records leave no gap.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Cursor {
    /// Filter the cursor was minted for; must match the current request.
    filter: KeyFilter,
    /// Last key of the preceding page; resume strictly after it.
    after: String,
}

const B64URL_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Encode bytes as unpadded base64url; output contains only
/// `[A-Za-z0-9_-]`.
fn base64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64URL_CHARS[((triple >> 18) & 0x3f) as usize] as char);
        out.push(B64URL_CHARS[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64URL_CHARS[((triple >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64URL_CHARS[(triple & 0x3f) as usize] as char);
        }
    }
    out
}

/// Decode unpadded base64url. Only `[A-Za-z0-9_-]` characters are accepted
/// (padding and any other byte are rejected); a dangling final quantum is
/// rejected.
fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let decode6 = |c: u8| -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    };
    let (chunks, rem) = bytes.as_chunks::<4>();
    for chunk in chunks {
        let n0 = decode6(chunk[0])?;
        let n1 = decode6(chunk[1])?;
        let n2 = decode6(chunk[2])?;
        let n3 = decode6(chunk[3])?;
        let triple = (n0 << 18) | (n1 << 12) | (n2 << 6) | n3;
        out.push((triple >> 16) as u8);
        out.push((triple >> 8) as u8);
        out.push(triple as u8);
    }
    // One leftover char carries no byte; two carry one, three carry two.
    match rem.len() {
        0 => {}
        2 => {
            let n0 = decode6(rem[0])?;
            let n1 = decode6(rem[1])?;
            let triple = (n0 << 18) | (n1 << 12);
            out.push((triple >> 16) as u8);
        }
        3 => {
            let n0 = decode6(rem[0])?;
            let n1 = decode6(rem[1])?;
            let n2 = decode6(rem[2])?;
            let triple = (n0 << 18) | (n1 << 12) | (n2 << 6);
            out.push((triple >> 16) as u8);
            out.push((triple >> 8) as u8);
        }
        _ => return None,
    }
    Some(out)
}

/// Serialize a continuation cursor to its opaque token form.
fn encode_cursor(cursor: &Cursor) -> Option<String> {
    let json = serde_json::to_vec(cursor).ok()?;
    Some(base64url_encode(&json))
}

/// Parse an opaque continuation token. Any structural, encoding, or
/// semantic failure (including an empty `after`) yields an error that the
/// caller maps to `invalid_query`.
fn decode_cursor(token: &str) -> Result<Cursor, ()> {
    let raw = base64url_decode(token).ok_or(())?;
    let cursor: Cursor = serde_json::from_slice(&raw).map_err(|_| ())?;
    if !valid_identifier(&cursor.after) {
        return Err(());
    }
    // Sanity-check the embedded bounds the same way query parsing does.
    if let Some((bound, _)) = &cursor.filter.lower
        && !valid_identifier(bound)
    {
        return Err(());
    }
    if let Some((bound, _)) = &cursor.filter.upper
        && !valid_identifier(bound)
    {
        return Err(());
    }
    if let Some(prefix) = &cursor.filter.prefix
        && !valid_identifier(prefix)
    {
        return Err(());
    }
    Ok(cursor)
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

/// Maximum page size accepted by the list endpoint.
const MAX_LIMIT: usize = 1000;

/// Parsed list query: the key filter plus optional keyset pagination.
struct ListQuery {
    filter: KeyFilter,
    limit: Option<usize>,
    /// Raw continuation token, already validated as non-empty and
    /// `[A-Za-z0-9_-]`-only; decoded later against the current filter.
    cursor: Option<String>,
}

/// Parse the query string into a key filter and pagination parameters. Any
/// parameter outside the documented set (`prefix`, `gte`, `gt`, `lte`,
/// `lt`, `limit`, `cursor`), duplicates, conflicting bounds, values that are
/// not valid key fragments (non-empty, `[A-Za-z0-9_-]`), a `limit` outside
/// 1..=1000, or an empty/illegal `cursor` are rejected.
fn parse_list_query(query: &str) -> Result<ListQuery, ()> {
    let mut filter = KeyFilter::default();
    if query.is_empty() {
        return Ok(ListQuery {
            filter,
            limit: None,
            cursor: None,
        });
    }
    let mut gte: Option<String> = None;
    let mut gt: Option<String> = None;
    let mut lte: Option<String> = None;
    let mut lt: Option<String> = None;
    let mut limit: Option<usize> = None;
    let mut cursor: Option<String> = None;

    for pair in query.split('&') {
        let (raw_name, raw_value) = pair.split_once('=').ok_or(())?;
        let name = percent_decode(raw_name)?;
        let value = percent_decode(raw_value)?;
        match name.as_str() {
            "limit" => {
                // Strict decimal integer: digits only, no sign or other
                // characters; range-checked to 1..=1000 below.
                if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(());
                }
                let parsed: usize = value.parse().map_err(|_| ())?;
                if !(1..=MAX_LIMIT).contains(&parsed) {
                    return Err(());
                }
                if limit.is_some() {
                    return Err(());
                }
                limit = Some(parsed);
            }
            "cursor" => {
                // Non-empty, opaque, charset-restricted. Full structural
                // validation happens once the current filter is known.
                if value.is_empty()
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                {
                    return Err(());
                }
                if cursor.is_some() {
                    return Err(());
                }
                cursor = Some(value);
            }
            "prefix" | "gte" | "gt" | "lte" | "lt" => {
                if !valid_identifier(&value) {
                    return Err(());
                }
                let slot = match name.as_str() {
                    "prefix" => &mut filter.prefix,
                    "gte" => &mut gte,
                    "gt" => &mut gt,
                    "lte" => &mut lte,
                    "lt" => &mut lt,
                    _ => unreachable!(),
                };
                if slot.is_some() {
                    return Err(());
                }
                *slot = Some(value);
            }
            _ => return Err(()),
        }
    }

    if filter.prefix.is_some() && (gte.is_some() || gt.is_some() || lte.is_some() || lt.is_some()) {
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
    Ok(ListQuery {
        filter,
        limit,
        cursor,
    })
}

async fn list_records(
    State(state): State<AppState>,
    PathParam(name): PathParam<String>,
    uri: Uri,
) -> Response {
    if !valid_identifier(&name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    let query = match parse_list_query(uri.query().unwrap_or("")) {
        Ok(query) => query,
        Err(()) => return error_response(StatusCode::BAD_REQUEST, "invalid_query"),
    };

    // A continuation token is accepted only when it decodes and pins the
    // exact filter of this request; it then marks the resume position.
    let after: Option<String> = match query.cursor.as_deref() {
        None => None,
        Some(token) => match decode_cursor(token) {
            Ok(cursor) if cursor.filter == query.filter => Some(cursor.after),
            // Corrupt token, unparseable payload, or filter mismatch.
            _ => return error_response(StatusCode::BAD_REQUEST, "invalid_query"),
        },
    };

    // Shared guard across the whole scan: commits rename files one by one,
    // so without this a list could interleave a batch in progress.
    let lock = dataset_lock(&name).await;
    let _guard = lock.read().await;
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
        if !query.filter.matches(key) {
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

    // Keyset resume: drop everything up to and including `after`. Position
    // is derived from the key order rather than an offset, so records
    // inserted behind the cursor are simply never revisited and deleted
    // positions leave no gap.
    let mut start = 0usize;
    if let Some(after) = &after {
        // A genuine cursor always names a record the preceding page
        // returned: it must lie inside the current filter's hit set, exist
        // exactly in it, and have a non-empty tail (nextCursor is only ever
        // minted while more records remain). Anything else is a forged,
        // corrupt, or out-of-bounds token.
        if !query.filter.matches(after) {
            return error_response(StatusCode::BAD_REQUEST, "invalid_query");
        }
        // First index whose key is strictly greater than `after`.
        start = records.partition_point(|(key, _, _)| key.as_str() <= after.as_str());
        let points_at_real_record = start > 0
            && records
                .get(start - 1)
                .is_some_and(|(key, _, _)| key == after);
        if !points_at_real_record || start >= records.len() {
            return error_response(StatusCode::BAD_REQUEST, "invalid_query");
        }
    }

    let total = records.len();
    let end = match query.limit {
        Some(limit) => (start + limit).min(total),
        None => total,
    };

    let mut next_cursor: Option<String> = None;
    if let Some(limit) = query.limit
        && start + limit < total
    {
        let last_key = &records[end - 1].0;
        // A cursor that cannot be minted is an internal failure, not a
        // client error: fail the whole request rather than return a
        // unpageable slice.
        let token = match encode_cursor(&Cursor {
            filter: query.filter.clone(),
            after: last_key.clone(),
        }) {
            Some(token) => token,
            None => return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        next_cursor = Some(token);
    }

    let page: Vec<Value> = records[start..end]
        .iter()
        .map(|(key, value, modified)| {
            json!({
                "key": key,
                "value": value,
                "updatedAt": format_rfc3339_utc(*modified),
            })
        })
        .collect();

    // Legacy response shape (no limit, no cursor) carries exactly the two
    // fields; paged responses add nextCursor only while a tail remains.
    let mut body = json!({ "name": name, "records": page });
    if let Some(token) = next_cursor {
        body["nextCursor"] = Value::String(token);
    }
    Json(body).into_response()
}

// ---------------------------------------------------------------------------
// Batch writes
//
// A batch is staged in a sibling directory of the dataset before any record
// file is touched:
//
//   <data_dir>/.<name>.batch-<pid>-<n>/
//     MANIFEST          - JSON: dataset name, batch keys, per-key "existed"
//                         flag, and the batch commit time; durable before
//                         COMMIT so recovery never has to infer key state
//                         from partially moved directories
//     COMMIT            - present once the whole batch is durably staged
//     ABORT             - present once a rollback has been decided
//     new/<key>.json    - new payload, staged via temp file + fsync + rename
//     old/<key>.json    - hardlink to the pre-request file (existing keys)
//
// Records are switched into the dataset only after COMMIT is durable, and
// the dataset directory is fsynced once all renames are done. A crash before
// COMMIT leaves the dataset untouched; a crash afterwards is completed at
// startup (re-apply) or, if ABORT exists, rolled back. Old payloads are
// captured as hardlinks, so rolling back restores both content and inode
// metadata (notably updatedAt). MANIFEST records whether each key existed
// beforehand, making both resume paths idempotent regardless of where a
// previous attempt stopped.
// ---------------------------------------------------------------------------

const COMMIT_MARKER: &str = "COMMIT";
const ABORT_MARKER: &str = "ABORT";
const MANIFEST_FILE: &str = "MANIFEST";

/// Why a batch request was rejected.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchError {
    /// Malformed JSON envelope or entries.
    InvalidJson,
    /// An entry key (already structurally present) violates identifier rules.
    InvalidIdentifier,
}

/// Validate and parse a batch request body into `(key, value)` pairs.
fn parse_batch_records(body: &[u8]) -> Result<Vec<(String, Value)>, BatchError> {
    if body.is_empty() {
        return Err(BatchError::InvalidJson);
    }
    let parsed: Value = serde_json::from_slice(body).map_err(|_| BatchError::InvalidJson)?;
    let Some(obj) = parsed.as_object() else {
        return Err(BatchError::InvalidJson);
    };
    // Missing `records`, or a non-array value, is invalid_json.
    let Some(arr) = obj.get("records").and_then(|v| v.as_array()) else {
        return Err(BatchError::InvalidJson);
    };

    let mut entries: Vec<(String, Value)> = Vec::with_capacity(arr.len());
    for item in arr {
        // Every entry must be an object with a string `key` and a present
        // `value` (which may itself be null).
        let Some(item) = item.as_object() else {
            return Err(BatchError::InvalidJson);
        };
        let Some(key) = item.get("key").and_then(|k| k.as_str()) else {
            return Err(BatchError::InvalidJson);
        };
        let Some(value) = item.get("value") else {
            return Err(BatchError::InvalidJson);
        };
        entries.push((key.to_owned(), value.clone()));
    }

    // A key that violates the identifier charset is invalid_identifier.
    if entries.iter().any(|(key, _)| !valid_identifier(key)) {
        return Err(BatchError::InvalidIdentifier);
    }

    // Duplicate keys within one batch are invalid_json.
    let mut seen = HashSet::with_capacity(entries.len());
    for (key, _) in &entries {
        if !seen.insert(key.as_str()) {
            return Err(BatchError::InvalidJson);
        }
    }
    Ok(entries)
}

async fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    tokio::fs::File::open(dir).await?.sync_all().await
}

/// Set a file's mtime and fsync it off the async runtime's worker threads
/// (set_modified/fsync are blocking syscalls).
async fn stamp_mtime_durable(path: PathBuf, at: SystemTime) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        file.set_modified(at)?;
        file.sync_all()
    })
    .await
    .map_err(std::io::Error::other)??;
    Ok(())
}

/// Staging directory for a batch: a sibling of the dataset directory.
fn staging_dir(state: &AppState, name: &str) -> PathBuf {
    state.data_dir.join(format!(
        ".{name}.batch-{}-{}",
        std::process::id(),
        BATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// One batch key as recorded in MANIFEST.
#[derive(serde::Serialize, serde::Deserialize)]
struct ManifestKey {
    key: String,
    /// Whether the record existed (and was snapshotted into `old/`) before
    /// the batch.
    existed: bool,
}

/// On-disk MANIFEST describing a staged batch.
#[derive(serde::Serialize, serde::Deserialize)]
struct Manifest {
    name: String,
    commit_at_ms: u128,
    keys: Vec<ManifestKey>,
}

impl Manifest {
    fn commit_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(self.commit_at_ms as u64)
    }
}

/// A staged batch ready to be switched into place.
struct StagedBatch {
    dir: PathBuf,
    manifest: Manifest,
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Snapshot existing records and stage every new payload, write MANIFEST and
/// then the durable COMMIT marker. No dataset record is modified until this
/// returns. On error the staging directory is removed; the dataset is left
/// untouched.
async fn stage_batch(
    state: &AppState,
    name: &str,
    entries: &[(String, Value)],
) -> std::io::Result<StagedBatch> {
    let dir = staging_dir(state, name);
    let new_dir = dir.join("new");
    let old_dir = dir.join("old");
    let setup = async {
        tokio::fs::create_dir_all(&new_dir).await?;
        tokio::fs::create_dir_all(&old_dir).await?;
        // Make the staging directory itself durable.
        fsync_dir(state.data_dir.as_ref()).await?;
        Ok::<(), std::io::Error>(())
    }
    .await;
    if let Err(e) = setup {
        let _ = tokio::fs::remove_dir_all(&dir).await;
        return Err(e);
    }

    let result = async {
        let mut manifest_keys = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            // Snapshot the pre-request file, if any. A hardlink keeps the
            // exact inode (content and mtime) alive for rollback.
            let target = record_path(state, name, key);
            let snapshot = old_dir.join(format!("{key}.json"));
            let existed = match tokio::fs::hard_link(&target, &snapshot).await {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => return Err(e),
            };
            manifest_keys.push(ManifestKey {
                key: key.clone(),
                existed,
            });

            let body = serde_json::to_vec(value).map_err(std::io::Error::other)?;
            write_file_atomic_in(&new_dir, &new_dir.join(format!("{key}.json")), &body).await?;
        }
        // Persist the snapshot hardlinks' directory entries; recovery may
        // need them to undo a committed batch.
        fsync_dir(&old_dir).await?;

        // The manifest (with the batch's commit timestamp) must be durable
        // before COMMIT: recovery relies on it to resume or undo the batch.
        let manifest = Manifest {
            name: name.to_owned(),
            commit_at_ms: now_epoch_ms(),
            keys: manifest_keys,
        };
        let manifest_body = serde_json::to_vec_pretty(&manifest).map_err(std::io::Error::other)?;
        write_file_atomic_in(&dir, &dir.join(MANIFEST_FILE), &manifest_body).await?;

        // Commit point: once this rename + fsync is durable, the batch must
        // either apply fully on every future startup or be explicitly
        // aborted.
        write_file_atomic_in(&dir, &dir.join(COMMIT_MARKER), b"committed\n").await?;
        Ok::<Manifest, std::io::Error>(manifest)
    }
    .await;

    match result {
        Ok(manifest) => Ok(StagedBatch { dir, manifest }),
        Err(e) => {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Err(e)
        }
    }
}

/// Rename every staged payload into the dataset, stamp each record's mtime
/// with the batch commit time (updatedAt is the write-completion moment),
/// fsync, then read each record back and verify the stored value. Returns
/// the verified `(key, value, modified)` rows.
async fn switch_batch(
    state: &AppState,
    name: &str,
    staged: &StagedBatch,
    entries: &[(String, Value)],
) -> std::io::Result<Vec<(String, Value, SystemTime)>> {
    let dir = ensure_dataset_dir_raw(state, name).await?;
    let new_dir = staged.dir.join("new");
    let commit_time = staged.manifest.commit_time();

    for item in &staged.manifest.keys {
        let from = new_dir.join(format!("{}.json", item.key));
        let to = record_path(state, name, &item.key);
        // rename(2) atomically replaces the destination on Unix.
        tokio::fs::rename(&from, &to).await?;
    }
    // One fsync after all renames makes the whole switch durable together.
    fsync_dir(&dir).await?;
    // The dataset directory may itself be new; persist its parent entry too.
    fsync_dir(state.data_dir.as_ref()).await?;

    let expected: HashMap<&str, &Value> = entries.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let mut verified = Vec::with_capacity(staged.manifest.keys.len());
    for item in &staged.manifest.keys {
        let key = item.key.as_str();
        let path = record_path(state, name, key);

        // Rename preserves the staged file's mtime; stamp it with the batch
        // commit time so updatedAt reflects the write completion.
        stamp_mtime_durable(path.clone(), commit_time).await?;

        let bytes = tokio::fs::read(&path).await?;
        let modified = tokio::fs::metadata(&path).await?.modified()?;
        let stored: Value = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        if Some(&stored) != expected.get(key).copied() {
            return Err(std::io::Error::other(format!(
                "post-commit verification mismatch for {name}/{key}"
            )));
        }
        verified.push((item.key.clone(), stored, modified));
    }
    Ok(verified)
}

async fn ensure_dataset_dir_raw(state: &AppState, name: &str) -> std::io::Result<PathBuf> {
    let dir = state.data_dir.join(name);
    tokio::fs::create_dir_all(&dir).await?;
    Ok(dir)
}

/// Durably abandon a staged batch: mark ABORT, restore every snapshot and
/// delete newly created records, fsync the dataset, then discard the
/// journal. Idempotent, so it can be re-driven by crash recovery.
async fn abort_batch(state: &AppState, name: &str, staged: &StagedBatch) -> std::io::Result<()> {
    // The ABORT marker makes the rollback decision durable before any
    // dataset file is touched.
    let abort_path = staged.dir.join(ABORT_MARKER);
    if tokio::fs::try_exists(&abort_path).await.unwrap_or(false) {
        // Already decided; nothing to write.
    } else {
        write_file_atomic_in(&staged.dir, &abort_path, b"aborted\n").await?;
    }

    let old_dir = staged.dir.join("old");
    let mut first_err: Option<std::io::Error> = None;
    for item in &staged.manifest.keys {
        let target = record_path(state, name, &item.key);
        if item.existed {
            let snapshot = old_dir.join(format!("{}.json", item.key));
            // Restore the old inode (content + mtime). A missing snapshot
            // means a previous attempt already restored it.
            match tokio::fs::rename(&snapshot, &target).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) if first_err.is_none() => first_err = Some(e),
                Err(_) => {}
            }
        } else {
            // Newly created by the batch; remove it. NotFound means an
            // earlier attempt already removed it.
            match tokio::fs::remove_file(&target).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) if first_err.is_none() => first_err = Some(e),
                Err(_) => {}
            }
        }
    }
    if let Err(e) = fsync_dir(&state.data_dir.join(name)).await
        && first_err.is_none()
    {
        first_err = Some(e);
    }
    if let Err(e) = fsync_dir(state.data_dir.as_ref()).await
        && first_err.is_none()
    {
        first_err = Some(e);
    }

    finish_batch_cleanup(&staged.dir, &abort_path).await;

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Retire the journal markers durably, then remove the staging directory.
/// Shared by the commit and abort paths.
async fn finish_batch_cleanup(staging: &Path, abort_path: &Path) {
    let _ = tokio::fs::remove_file(staging.join(COMMIT_MARKER)).await;
    let _ = tokio::fs::remove_file(abort_path).await;
    let _ = tokio::fs::remove_file(staging.join(MANIFEST_FILE)).await;
    let _ = fsync_dir(staging).await;
    let _ = tokio::fs::remove_dir_all(staging).await;
}

/// Remove the journal of a successfully applied batch. Errors here are not
/// fatal to the request (the batch is already durable); startup recovery
/// sweeps anything left behind.
async fn discard_batch(staged: &StagedBatch) {
    finish_batch_cleanup(&staged.dir, &staged.dir.join(ABORT_MARKER)).await;
}

async fn put_records(
    State(state): State<AppState>,
    PathParam(name): PathParam<String>,
    body: Bytes,
) -> Response {
    if !valid_identifier(&name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    let entries = match parse_batch_records(&body) {
        Ok(entries) => entries,
        Err(BatchError::InvalidIdentifier) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
        }
        Err(BatchError::InvalidJson) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_json");
        }
    };

    // An empty batch is a legal no-op: 200 with an empty records array.
    if entries.is_empty() {
        return Json(json!({ "name": name, "records": [] })).into_response();
    }

    // Exclusive with every other read and write of this dataset for the
    // duration of the commit.
    let lock = dataset_lock(&name).await;
    let _guard = lock.write().await;

    let staged = match stage_batch(&state, &name, &entries).await {
        Ok(staged) => staged,
        Err(e) => {
            tracing_error(&e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };

    let verified = match switch_batch(&state, &name, &staged, &entries).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing_error(&e);
            // Restore the exact pre-request dataset; if even that fails the
            // on-disk journal is re-driven (towards abort) at next startup.
            if let Err(rollback_err) = abort_batch(&state, &name, &staged).await {
                tracing_error(&rollback_err);
            }
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };

    discard_batch(&staged).await;

    // Byte-wise order on UTF-8 is Unicode code-point order.
    let mut verified = verified;
    verified.sort_by(|a, b| a.0.cmp(&b.0));
    let records: Vec<Value> = verified
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

/// Finish or undo one staging directory found at startup. MANIFEST drives
/// every decision, so resume work is idempotent no matter how far a prior
/// attempt (live or recovery) got.
async fn recover_one_batch(data_dir: &Path, staging: &Path) {
    let file_name = match staging.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_owned(),
        None => {
            let _ = tokio::fs::remove_dir_all(staging).await;
            return;
        }
    };
    // Layout: .<name>.batch-<pid>-<n>, identifiers are [A-Za-z0-9_-].
    let marked = file_name.strip_prefix('.').unwrap_or(&file_name);
    let Some((_dir_name, _)) = marked.split_once(".batch-") else {
        let _ = tokio::fs::remove_dir_all(staging).await;
        return;
    };

    // Crashed before/while writing MANIFEST or COMMIT: no record was ever
    // renamed, so the dataset is untouched and the journal can be discarded.
    let manifest_path = staging.join(MANIFEST_FILE);
    let commit_path = staging.join(COMMIT_MARKER);
    let abort_path = staging.join(ABORT_MARKER);
    let has_manifest = tokio::fs::try_exists(&manifest_path).await.unwrap_or(false);
    let has_commit = tokio::fs::try_exists(&commit_path).await.unwrap_or(false);
    if !has_manifest || !has_commit {
        let _ = tokio::fs::remove_dir_all(staging).await;
        return;
    }

    let manifest_bytes = match tokio::fs::read(&manifest_path).await {
        Ok(b) => b,
        // A torn manifest means the batch never reached its commit point;
        // the dataset was not modified.
        Err(_) => {
            let _ = tokio::fs::remove_dir_all(staging).await;
            return;
        }
    };
    let manifest: Manifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(_) => {
            let _ = tokio::fs::remove_dir_all(staging).await;
            return;
        }
    };
    let name = manifest.name.clone();
    let dataset_dir = data_dir.join(&name);
    let has_abort = tokio::fs::try_exists(&abort_path).await.unwrap_or(false);
    let new_dir = staging.join("new");
    let old_dir = staging.join("old");

    if has_abort {
        // Resume an interrupted rollback.
        let _ = tokio::fs::create_dir_all(&dataset_dir).await;
        for item in &manifest.keys {
            let target = dataset_dir.join(format!("{}.json", item.key));
            if item.existed {
                let snapshot = old_dir.join(format!("{}.json", item.key));
                // Missing snapshot: already restored in an earlier pass.
                let _ = tokio::fs::rename(&snapshot, &target).await;
            } else {
                let _ = tokio::fs::remove_file(&target).await;
            }
        }
        let _ = fsync_dir(&dataset_dir).await;
        let _ = fsync_dir(data_dir).await;
    } else {
        // Resume an interrupted commit. Both moves and the mtime stamping
        // are re-applied, so an already-finished switch is a no-op.
        if let Err(e) = tokio::fs::create_dir_all(&dataset_dir).await {
            eprintln!("recovery: cannot create dataset {name}: {e}");
            return;
        }
        let commit_time = manifest.commit_time();
        for item in &manifest.keys {
            let target = dataset_dir.join(format!("{}.json", item.key));
            let staged_new = new_dir.join(format!("{}.json", item.key));
            // Missing staged file: already moved in an earlier pass.
            if let Err(e) = tokio::fs::rename(&staged_new, &target).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("recovery: cannot apply {}/{}: {e}", name, item.key);
            }
            // Restore the durable commit mtime whether this pass moved the
            // file or an earlier one did.
            if let Ok(file) = std::fs::File::open(&target) {
                let _ = file.set_modified(commit_time);
                let _ = file.sync_all();
            }
        }
        let _ = fsync_dir(&dataset_dir).await;
        let _ = fsync_dir(data_dir).await;
    }

    // The dataset is now in a complete state; retire the journal.
    finish_batch_cleanup(staging, &abort_path).await;
}

/// Sweep staging directories left by processes that died mid-batch. Runs
/// once before the server starts accepting requests.
async fn recover_pending_batches(state: &AppState) {
    let mut entries = match tokio::fs::read_dir(state.data_dir.as_ref()).await {
        Ok(entries) => entries,
        // A fresh deployment has no data directory yet.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            eprintln!("recovery: cannot scan data dir: {e}");
            return;
        }
    };
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) => {
                eprintln!("recovery: cannot enumerate data dir: {e}");
                break;
            }
        };
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if file_name.starts_with('.') && file_name.contains(".batch-") {
            recover_one_batch(state.data_dir.as_ref(), &entry.path()).await;
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

    // Finish or undo any batch left in flight by a previous process before
    // serving traffic.
    recover_pending_batches(&state).await;

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route(
            "/datasets/{name}/records/{key}",
            put(put_record).get(get_record),
        )
        .route(
            "/datasets/{name}/records",
            get(list_records).put(put_records),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_round_trip() {
        // Include every length mod 3 so every padding path is exercised.
        for payload in [
            Vec::new(),
            vec![0x00],
            vec![0x00, 0x01],
            vec![0x00, 0x01, 0x02],
            (0u8..=255).collect(),
            serde_json::to_vec("héllo—🚀").unwrap(),
        ] {
            let token = base64url_encode(&payload);
            assert!(
                token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "token must stay within [A-Za-z0-9_-]"
            );
            assert!(!token.contains('='));
            assert_eq!(
                base64url_decode(&token).as_deref(),
                Some(payload.as_slice())
            );
        }
    }

    #[test]
    fn base64url_rejects_garbage() {
        // Padding, illegal alphabet, and dangling one-char quanta.
        for bad in ["=", "AB==", "abc!", "ab*", "a", "abcde=", "++//", " "] {
            assert!(base64url_decode(bad).is_none(), "should reject {bad:?}");
        }
    }

    #[test]
    fn cursor_round_trip() {
        let cursor = Cursor {
            filter: KeyFilter {
                prefix: Some("user-".to_owned()),
                lower: None,
                upper: Some(("z".to_owned(), false)),
            },
            after: "user-42".to_owned(),
        };
        let token = encode_cursor(&cursor).unwrap();
        assert_eq!(decode_cursor(&token).unwrap(), cursor);
    }

    #[test]
    fn cursor_rejects_bad_payloads() {
        // Empty `after`, illegal embedded fragments, non-JSON tokens.
        assert!(decode_cursor("AAAA").is_err());
        let empty_after = encode_cursor(&Cursor {
            filter: KeyFilter::default(),
            after: String::new(),
        })
        .unwrap();
        assert!(decode_cursor(&empty_after).is_err());
    }

    #[test]
    fn list_query_accepts() {
        let q = parse_list_query("prefix=user-&limit=50").unwrap();
        assert_eq!(q.limit, Some(50));
        assert_eq!(q.filter.prefix.as_deref(), Some("user-"));
        assert!(q.cursor.is_none());

        let q = parse_list_query("gte=a&lt=z&limit=1000&cursor=Abc_-9").unwrap();
        assert_eq!(q.limit, Some(1000));
        assert_eq!(q.filter.lower, Some(("a".to_owned(), true)));
        assert_eq!(q.filter.upper, Some(("z".to_owned(), false)));
        assert_eq!(q.cursor.as_deref(), Some("Abc_-9"));

        // No params preserves the legacy full-list request.
        let q = parse_list_query("").unwrap();
        assert!(q.limit.is_none() && q.cursor.is_none());
    }

    #[test]
    fn list_query_rejects() {
        let bad = [
            "limit=0",
            "limit=1001",
            "limit=-5",
            "limit=abc",
            "limit=1&limit=2",
            "limit=99999999999999999999",
            "cursor=",
            "cursor=bad.token",
            "cursor=a&cursor=b",
            "unknown=1",
            "prefix=p&gte=a",
            "gte=a&gt=b",
            "lte=a&lt=b",
            "prefix=",
            "limit=",
        ];
        for case in bad {
            assert!(parse_list_query(case).is_err(), "should reject {case:?}");
        }
    }
}
