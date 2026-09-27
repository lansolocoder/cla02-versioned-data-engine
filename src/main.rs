use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
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
    path::{Path as StdPath, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
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

/// Serializes writers against readers within a dataset: a batch (or a single
/// PUT) holds a write guard while committing, so concurrent GET/list
/// requests observe only the pre-batch or post-batch dataset, never a
/// partially written one.
type DatasetLock = tokio::sync::RwLock<()>;

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
    /// Per-dataset reader/writer exclusion, created on demand.
    locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<DatasetLock>>>>,
}

impl AppState {
    async fn dataset_lock(&self, name: &str) -> Arc<DatasetLock> {
        let mut guards = self.locks.lock().await;
        guards
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(DatasetLock::new(())))
            .clone()
    }
}

static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);

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

    let lock = state.dataset_lock(&name).await;
    let _guard = lock.write().await;
    if let Err(e) = recover_dataset(&state.data_dir.join(&name)).await {
        tracing_error(&e);
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    }
    if let Err(resp) = persist_record(&state, &name, &key, &body).await {
        return resp;
    }
    Json(json!({ "key": key, "value": value })).into_response()
}

#[derive(Deserialize)]
struct BatchRequestBody {
    records: Vec<BatchRecordInput>,
}

#[derive(Deserialize)]
struct BatchRecordInput {
    key: String,
    value: Value,
}

/// PUT /datasets/{name}/records — write a whole batch atomically: either
/// every record lands or the dataset is left byte-for-byte as it was.
async fn put_records_batch(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    if !valid_identifier(&name) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }
    if body.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "invalid_json");
    }
    // Missing/non-array `records`, non-object entries, a missing/wrong-typed
    // `key` or a missing `value` all fail to deserialize.
    let parsed: BatchRequestBody = match serde_json::from_slice(&body) {
        Ok(parsed) => parsed,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_json"),
    };

    // Identifier rule applies to every key in the batch.
    for record in &parsed.records {
        if !valid_identifier(&record.key) {
            return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
        }
    }
    // A key may appear at most once per batch.
    let mut seen = HashSet::new();
    for record in &parsed.records {
        if !seen.insert(record.key.as_str()) {
            return error_response(StatusCode::BAD_REQUEST, "invalid_json");
        }
    }

    let mut entries: Vec<(String, Value)> = parsed
        .records
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect();

    let lock = state.dataset_lock(&name).await;
    let _guard = lock.write().await;

    // Finish any transaction interrupted in a previous process.
    if let Err(e) = recover_dataset(&state.data_dir.join(&name)).await {
        tracing_error(&e);
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    }

    // An empty batch is valid and changes nothing, not even the dataset
    // directory's existence.
    if entries.is_empty() {
        return Json(json!({ "name": name, "records": [] })).into_response();
    }

    let applied = match persist_batch(&state, &name, &entries).await {
        Ok(applied) => applied,
        Err(resp) => return resp,
    };
    let timestamps: HashMap<String, SystemTime> = applied.into_iter().collect();

    // Byte-wise order on UTF-8 is Unicode code-point order.
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let records: Vec<Value> = entries
        .into_iter()
        .map(|(key, value)| {
            json!({
                "key": key,
                "value": value,
                "updatedAt": format_rfc3339_utc(timestamps[&key]),
            })
        })
        .collect();
    Json(json!({ "name": name, "records": records })).into_response()
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

/// One staged record ready to be installed: key, its completion timestamp
/// and the record's prior on-disk mtime, if it already existed.
struct StagedRecord {
    key: String,
    when: SystemTime,
    old_mtime: Option<SystemTime>,
}

/// Manifest entry persisted before the commit marker, naming every key in
/// the batch and its pre-batch mtime (null for keys that did not exist).
fn manifest_json(staged: &[StagedRecord]) -> Value {
    let keys: Vec<Value> = staged
        .iter()
        .map(|r| {
            json!({
                "key": r.key,
                "old": r.old_mtime.map(systemtime_nanos),
            })
        })
        .collect();
    json!({ "keys": keys })
}

fn systemtime_nanos(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Write a whole batch through a crash-safe transaction:
///
/// 1. In a private `.txn-*` directory, stage every new file (content and
///    completion mtime fsynced) and a byte+mtime copy of every pre-existing
///    record, plus a manifest naming the whole batch. Nothing in the dataset
///    directory has changed yet.
/// 2. Fsync a `COMMITTED` marker: this is the single commit point.
/// 3. Rename each staged file over its target, then fsync the dataset dir.
///
/// Recovery at startup reconciles any staging directory left by a crash:
/// no `COMMITTED` → discard (nothing was installed); `COMMITTED` without
/// `ABORTED` → redo the remaining renames (whole batch takes effect);
/// `COMMITTED` + `ABORTED` → restore every key from the manifest/backups
/// (whole batch disappears). Both outcomes are complete dataset states.
///
/// Any in-process failure before commit aborts with the dataset untouched;
/// a failure after commit durably marks `ABORTED` and restores the pre-batch
/// bytes and mtime.
async fn persist_batch(
    state: &AppState,
    name: &str,
    entries: &[(String, Value)],
) -> Result<Vec<(String, SystemTime)>, Response> {
    let internal = || error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    let dir = state.data_dir.join(name);
    let ds_existed = tokio::fs::metadata(&dir).await.is_ok();
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        tracing_error(&e);
        return Err(internal());
    }

    let txn_id = TXN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let staging = dir.join(format!(".txn-{}-{}", std::process::id(), txn_id));
    let staged_dir = staging.join("new");
    let backup_dir = staging.join("old");
    let committed = staging.join("COMMITTED");
    let aborted = staging.join("ABORTED");
    // Best-effort cleanup runs on every exit path; a staging dir left behind
    // by a crash is reconciled by `recover_all_datasets` at next startup.
    let cleanup = Cleanup::new(staging.clone(), dir.clone(), ds_existed);

    if let Err(e) = tokio::fs::create_dir_all(&staged_dir).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }
    if let Err(e) = tokio::fs::create_dir_all(&backup_dir).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }
    // Make the staging directory entry itself durable before anything inside
    // it becomes the commit point.
    if let Err(e) = fsync_dir(&dir).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }

    // Stage all writes and capture pre-state first; no target is touched yet.
    let mut staged: Vec<StagedRecord> = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let body = serde_json::to_vec(value).map_err(|e| {
            eprintln!("encode record {name}/{key}: {e}");
            internal()
        })?;
        let target = record_path(state, name, key);
        let old_mtime = match tokio::fs::read(&target).await {
            Ok(bytes) => {
                let meta = tokio::fs::metadata(&target).await.map_err(|e| {
                    tracing_error(&e);
                    internal()
                })?;
                let modified = meta.modified().map_err(|e| {
                    tracing_error(&e);
                    internal()
                })?;
                // Persist the backup with the original mtime stamped on its
                // inode, so an undo restores content and timestamp exactly.
                if let Err(e) = stage_file(
                    &backup_dir.join(format!("{key}.json")),
                    &bytes,
                    Some(modified),
                )
                .await
                {
                    tracing_error(&e);
                    cleanup.rm_rf().await;
                    return Err(internal());
                }
                Some(modified)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing_error(&e);
                cleanup.rm_rf().await;
                return Err(internal());
            }
        };
        let staged_path = staged_dir.join(format!("{key}.json"));
        // The completion timestamp is assigned before commit and stamped on
        // the staged inode: the later rename carries this mtime to its final
        // location, so redo after a crash produces the same updatedAt the
        // response reported.
        let when = SystemTime::now();
        if let Err(e) = stage_file(&staged_path, &body, Some(when)).await {
            tracing_error(&e);
            cleanup.rm_rf().await;
            return Err(internal());
        }
        staged.push(StagedRecord {
            key: key.clone(),
            when,
            old_mtime,
        });
    }

    // Manifest names the whole batch so an abort/undo can restore (or delete)
    // keys even after some staged inodes were consumed by installs.
    let manifest = serde_json::to_vec(&manifest_json(&staged)).map_err(|_| internal())?;
    if let Err(e) = stage_file(&staging.join("manifest.json"), &manifest, None).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }

    // Fsync the staging subdirectories so staged files, backups and manifest
    // are durable, then persist the commit marker: this is the commit point.
    if let Err(e) = fsync_dir(&staged_dir).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }
    if let Err(e) = fsync_dir(&backup_dir).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }
    if let Err(e) = stage_file(&committed, b"committed\n", None).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }
    if let Err(e) = fsync_dir(&staging).await {
        tracing_error(&e);
        cleanup.rm_rf().await;
        return Err(internal());
    }

    // Install. The staged inode already carries this record's completion
    // mtime, and the rename moves that inode onto the target name.
    let mut applied: Vec<(String, SystemTime)> = Vec::new();
    for rec in &staged {
        let target = record_path(state, name, rec.key.as_str());
        if let Err(e) =
            tokio::fs::rename(staged_dir.join(format!("{}.json", rec.key)), &target).await
        {
            tracing_error(&e);
            // If the undo fully completed, discard the staging directory;
            // otherwise keep it so startup recovery finishes the restore.
            if abort_txn(state, name, &staging, &aborted).await {
                cleanup.rm_rf().await;
            } else {
                cleanup.forget();
            }
            return Err(internal());
        }
        applied.push((rec.key.clone(), rec.when));
    }

    if let Err(e) = fsync_dir(&dir).await {
        tracing_error(&e);
        if abort_txn(state, name, &staging, &aborted).await {
            cleanup.rm_rf().await;
        } else {
            cleanup.forget();
        }
        return Err(internal());
    }
    cleanup.rm_rf().await;
    Ok(applied)
}

/// Durably mark a committed transaction aborted, then restore the dataset to
/// its pre-batch state. Returns true only if the restore itself completed, in
/// which case the staging directory may be discarded; on failure it is kept
/// on disk so startup recovery finishes the undo. If the abort marker cannot
/// be made durable a crash would instead be recovered forward (redo); either
/// outcome is a complete dataset state.
async fn abort_txn(state: &AppState, name: &str, staging: &StdPath, aborted: &StdPath) -> bool {
    let marker_ok = stage_file(aborted, b"aborted\n", None).await.is_ok();
    if marker_ok {
        let _ = fsync_dir(staging).await;
    }
    restore_txn(&state.data_dir.join(name), staging)
        .await
        .is_ok()
}

/// Undo a transaction using its manifest: pre-existing keys are restored from
/// their byte/mtime backups; keys absent before the batch are deleted. Copy
/// (not rename) keeps this re-runnable across repeated crash recovery. Safe to
/// call with no readers: recovery runs before serving and runtime abort runs
/// while holding the dataset's write lock.
async fn restore_txn(ds_dir: &StdPath, staging: &StdPath) -> std::io::Result<()> {
    let manifest = tokio::fs::read(staging.join("manifest.json")).await?;
    let manifest: Value = serde_json::from_slice(&manifest)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let backup_dir = staging.join("old");
    for entry in manifest["keys"].as_array().into_iter().flatten() {
        let Some(key) = entry["key"].as_str() else {
            continue;
        };
        let target = ds_dir.join(format!("{key}.json"));
        match entry["old"].as_u64() {
            Some(nanos) => {
                let backup = backup_dir.join(format!("{key}.json"));
                restore_file(&backup, &target, nanos).await?;
            }
            None => {
                let _ = tokio::fs::remove_file(&target).await;
            }
        }
    }
    fsync_dir(ds_dir).await?;
    Ok(())
}

/// Copy a backup over its target and stamp the pre-batch mtime, fsyncing both
/// contents and metadata. Blocking: one copy + two fsyncs under the lock.
async fn restore_file(backup: &StdPath, target: &StdPath, mtime_nanos: u64) -> std::io::Result<()> {
    let backup = backup.to_owned();
    let target = target.to_owned();
    tokio::task::spawn_blocking(move || {
        std::fs::copy(&backup, &target)?;
        let file = std::fs::File::open(&target)?;
        let when = SystemTime::UNIX_EPOCH + Duration::from_nanos(mtime_nanos);
        file.set_modified(when)?;
        file.sync_all()?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .expect("restore_file task")
}

/// Create a file with the given body and, optionally, an explicit mtime,
/// fsyncing so both contents and metadata change are durable. Runs on the
/// blocking pool: while it is cheap (one write + two fsyncs) holding the
/// batch lock, blocking a worker thread must not stall other tasks.
async fn stage_file(path: &StdPath, body: &[u8], mtime: Option<SystemTime>) -> std::io::Result<()> {
    let path = path.to_owned();
    let body = body.to_vec();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&body)?;
        file.sync_all()?;
        if let Some(when) = mtime {
            file.set_modified(when)?;
            // Persist the mtime change itself.
            file.sync_all()?;
        }
        Ok::<(), std::io::Error>(())
    })
    .await
    .expect("stage_file task")
}

/// fsync a directory so that file creations and renames inside it survive a
/// crash or power loss.
async fn fsync_dir(path: &StdPath) -> std::io::Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        file.sync_all()
    })
    .await
    .expect("fsync_dir task")
}

struct Cleanup {
    staging: PathBuf,
    /// The dataset directory, removed on cleanup only if it did not exist
    /// before this batch and is now empty (so a failed batch against a
    /// nonexistent dataset leaves no trace of it).
    ds_dir: PathBuf,
    ds_existed: bool,
    remove: std::sync::atomic::AtomicBool,
}

impl Cleanup {
    fn new(staging: PathBuf, ds_dir: PathBuf, ds_existed: bool) -> Self {
        Self {
            staging,
            ds_dir,
            ds_existed,
            remove: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Keep the staging directory on purpose (startup recovery will finish
    /// it); suppress the Drop-time best-effort deletion.
    fn forget(&self) {
        self.remove
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    async fn rm_rf(&self) {
        let _ = tokio::fs::remove_dir_all(&self.staging).await;
        if !self.ds_existed {
            // Only succeeds when empty; a successful or populated dataset is
            // left in place.
            let _ = tokio::fs::remove_dir(&self.ds_dir).await;
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        // Defense in depth if an exit path forgot to await cleanup.
        if self.remove.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_dir_all(&self.staging);
            if !self.ds_existed {
                let _ = std::fs::remove_dir(&self.ds_dir);
            }
        }
    }
}

/// Reconcile any staging directories left by a crash before the server
/// starts serving.
///
/// - No durable `COMMITTED`: the transaction never reached the commit point
///   and installed nothing → discard the staging directory.
/// - `COMMITTED` but no `ABORTED`: a forward crash mid-install → rename any
///   staged files not yet installed, so the whole batch takes effect.
/// - `COMMITTED` + `ABORTED`: an abort interrupted mid-undo → restore the
///   pre-batch state from the manifest and backups, so the batch disappears.
async fn recover_all_datasets(data_dir: &StdPath) -> std::io::Result<()> {
    let mut root = match tokio::fs::read_dir(data_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    loop {
        let entry = match root.next_entry().await? {
            Some(e) => e,
            None => break,
        };
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        recover_dataset(&entry.path()).await?;
    }
    Ok(())
}

/// Reconcile every staging directory left inside one dataset directory.
/// Safe to call while holding the dataset's write lock (no readers or other
/// writers can observe the intermediate state).
async fn recover_dataset(ds_dir: &StdPath) -> std::io::Result<()> {
    let mut rd = match tokio::fs::read_dir(ds_dir).await {
        Ok(rd) => rd,
        // An empty or absent dataset has nothing to recover.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    loop {
        let child = match rd.next_entry().await? {
            Some(c) => c,
            None => break,
        };
        let name = child.file_name();
        let name = name.to_string_lossy();
        let Some(rest) = name.strip_prefix(".txn-") else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let staging = child.path();
        let committed = staging.join("COMMITTED");
        let aborted = staging.join("ABORTED");
        if tokio::fs::metadata(&committed).await.is_ok() {
            if tokio::fs::metadata(&aborted).await.is_ok() {
                restore_txn(ds_dir, &staging).await?;
                let _ = tokio::fs::remove_dir_all(&staging).await;
            } else {
                redo_committed(ds_dir, &staging).await?;
            }
        } else {
            let _ = tokio::fs::remove_dir_all(&staging).await;
        }
    }
    Ok(())
}

/// Finish a committed transaction found during recovery by renaming every
/// staged file still in `new/` into the dataset directory. Files already
/// installed during the original install are absent here; their targets hold
/// the new inode already. Renames over existing files are the intended redo;
/// they are atomic and idempotent.
async fn redo_committed(ds_dir: &StdPath, staging: &StdPath) -> std::io::Result<()> {
    let new_dir = staging.join("new");
    let mut entries = match tokio::fs::read_dir(&new_dir).await {
        Ok(rd) => rd,
        Err(_) => return Ok(()),
    };
    loop {
        let entry = match entries.next_entry().await? {
            Some(e) => e,
            None => break,
        };
        let target = ds_dir.join(entry.file_name());
        tokio::fs::rename(entry.path(), &target).await?;
    }
    fsync_dir(ds_dir).await?;
    let _ = tokio::fs::remove_dir_all(staging).await;
    Ok(())
}

async fn get_record(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> Response {
    if !valid_identifier(&name) || !valid_identifier(&key) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_identifier");
    }

    let lock = state.dataset_lock(&name).await;
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

    let lock = state.dataset_lock(&name).await;
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
    if let Err(e) = tokio::fs::create_dir_all(&data_dir).await {
        eprintln!("storage error: {e}");
        return Err(e.into());
    }
    if let Err(e) = recover_all_datasets(&data_dir).await {
        eprintln!("recovery error: {e}");
        return Err(e.into());
    }
    let state = AppState {
        data_dir: Arc::new(data_dir),
        locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };

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
            get(list_records).put(put_records_batch),
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
