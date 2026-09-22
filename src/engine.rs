//! Stateless snapshot diffing and bidirectional replay.
//!
//! All diff/apply logic lives in this module; the HTTP layer only maps
//! JSON requests in and responses out.

use serde_json::{Map, Number, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

/// Error returned for malformed requests (400) and state conflicts (409).
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
    /// JSON-pointer-style locator of the offending request field.
    pub path: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            status: 400,
            code: "bad_request",
            message: message.into(),
            path: path.into(),
        }
    }

    fn conflict(message: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            status: 409,
            code: "conflict",
            message: message.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    id: String,
    values: Map<String, Value>,
}

#[derive(Debug, Clone)]
struct FieldChange {
    /// RFC 6901 JSON Pointer as emitted in diffs / accepted in applies.
    pointer: String,
    /// Unescaped path segments, used to navigate row values.
    segments: Vec<String>,
    /// `None` means the field is absent on that side (distinct from null).
    old: Option<Value>,
    new: Option<Value>,
}

#[derive(Debug, Clone)]
enum Change {
    Add { id: String, value: Map<String, Value> },
    Remove { id: String, value: Map<String, Value> },
    Update { id: String, fields: Vec<FieldChange> },
}

impl Change {
    fn id(&self) -> &str {
        match self {
            Change::Add { id, .. } | Change::Remove { id, .. } | Change::Update { id, .. } => id,
        }
    }
}

/// Handle `POST /diff`: compute the changes between two row snapshots.
pub fn diff_endpoint(body: &[u8]) -> Result<Value, ApiError> {
    let request = parse_body(body)?;
    let object = as_object(&request, "")?;
    let before = parse_rows(required(object, "before")?, "/before")?;
    let after = parse_rows(required(object, "after")?, "/after")?;
    let changes = compute_diff(&before, &after);
    Ok(json!({ "changes": changes.iter().map(change_to_json).collect::<Vec<_>>() }))
}

/// Handle `POST /apply`: replay changes over rows in either direction.
pub fn apply_endpoint(body: &[u8]) -> Result<Value, ApiError> {
    let request = parse_body(body)?;
    let object = as_object(&request, "")?;
    let rows = parse_rows(required(object, "rows")?, "/rows")?;
    let changes = parse_changes(required(object, "changes")?, "/changes")?;
    let direction = match required(object, "direction")? {
        Value::String(s) if s == "forward" => Direction::Forward,
        Value::String(s) if s == "reverse" => Direction::Reverse,
        Value::String(_) => {
            return Err(ApiError::bad_request(
                "direction must be \"forward\" or \"reverse\"",
                "/direction",
            ));
        }
        _ => {
            return Err(ApiError::bad_request(
                "direction must be a string",
                "/direction",
            ));
        }
    };
    let rows = apply_changes(rows, &changes, direction)?;
    Ok(json!({ "rows": rows.iter().map(row_to_json).collect::<Vec<_>>() }))
}

fn parse_body(body: &[u8]) -> Result<Value, ApiError> {
    serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON body: {e}"), ""))
}

fn as_object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, ApiError> {
    value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("expected a JSON object", path))
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ApiError> {
    object
        .get(key)
        .ok_or_else(|| ApiError::bad_request(format!("missing required field \"{key}\""), format!("/{key}")))
}

fn parse_id(object: &Map<String, Value>, base: &str) -> Result<String, ApiError> {
    let path = format!("{base}/id");
    match object.get("id") {
        None => Err(ApiError::bad_request("missing required field \"id\"", path)),
        Some(Value::String(id)) if id.is_empty() => {
            Err(ApiError::bad_request("id must be a non-empty string", path))
        }
        Some(Value::String(id)) => Ok(id.clone()),
        Some(_) => Err(ApiError::bad_request("id must be a non-empty string", path)),
    }
}

fn parse_rows(value: &Value, path: &str) -> Result<Vec<Row>, ApiError> {
    let items = value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("expected an array of rows", path))?;
    let mut rows = Vec::with_capacity(items.len());
    let mut seen = HashSet::new();
    for (i, item) in items.iter().enumerate() {
        let base = format!("{path}/{i}");
        let object = as_object(item, &base)?;
        let id = parse_id(object, &base)?;
        let values_path = format!("{base}/values");
        let values = match object.get("values") {
            None => return Err(ApiError::bad_request("missing required field \"values\"", values_path)),
            Some(Value::Object(values)) => values.clone(),
            Some(_) => return Err(ApiError::bad_request("values must be an object", values_path)),
        };
        if !seen.insert(id.clone()) {
            return Err(ApiError::bad_request(
                format!("duplicate row id \"{id}\""),
                format!("{base}/id"),
            ));
        }
        rows.push(Row { id, values });
    }
    Ok(rows)
}

fn parse_changes(value: &Value, path: &str) -> Result<Vec<Change>, ApiError> {
    let items = value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("expected an array of changes", path))?;
    let mut changes = Vec::with_capacity(items.len());
    let mut seen = HashSet::new();
    for (i, item) in items.iter().enumerate() {
        let base = format!("{path}/{i}");
        let object = as_object(item, &base)?;
        let id = parse_id(object, &base)?;
        if !seen.insert(id.clone()) {
            return Err(ApiError::bad_request(
                format!("duplicate change for id \"{id}\""),
                format!("{base}/id"),
            ));
        }
        let op_path = format!("{base}/op");
        let op = match object.get("op") {
            None => return Err(ApiError::bad_request("missing required field \"op\"", op_path)),
            Some(Value::String(op)) => op.as_str(),
            Some(_) => return Err(ApiError::bad_request("op must be a string", op_path)),
        };
        let change = match op {
            "add" | "remove" => {
                let value_path = format!("{base}/value");
                let value = match object.get("value") {
                    None => {
                        return Err(ApiError::bad_request("missing required field \"value\"", value_path));
                    }
                    Some(Value::Object(value)) => value.clone(),
                    Some(_) => {
                        return Err(ApiError::bad_request("value must be an object", value_path));
                    }
                };
                if op == "add" {
                    Change::Add { id, value }
                } else {
                    Change::Remove { id, value }
                }
            }
            "update" => {
                let fields_path = format!("{base}/fields");
                let fields_value = object
                    .get("fields")
                    .ok_or_else(|| ApiError::bad_request("missing required field \"fields\"", &fields_path))?;
                let fields_items = fields_value
                    .as_array()
                    .ok_or_else(|| ApiError::bad_request("fields must be an array", &fields_path))?;
                let mut fields = Vec::with_capacity(fields_items.len());
                let mut paths = HashSet::new();
                for (j, field_item) in fields_items.iter().enumerate() {
                    let field_base = format!("{fields_path}/{j}");
                    let field = as_object(field_item, &field_base)?;
                    let pointer_path = format!("{field_base}/path");
                    let pointer = match field.get("path") {
                        None => {
                            return Err(ApiError::bad_request("missing required field \"path\"", pointer_path));
                        }
                        Some(Value::String(pointer)) => pointer.clone(),
                        Some(_) => {
                            return Err(ApiError::bad_request("path must be a string", pointer_path));
                        }
                    };
                    let segments = parse_pointer(&pointer)
                        .map_err(|message| ApiError::bad_request(message, &pointer_path))?;
                    if !paths.insert(pointer.clone()) {
                        return Err(ApiError::bad_request(
                            format!("duplicate path \"{pointer}\""),
                            pointer_path,
                        ));
                    }
                    fields.push(FieldChange {
                        pointer,
                        segments,
                        old: field.get("old").cloned(),
                        new: field.get("new").cloned(),
                    });
                }
                Change::Update { id, fields }
            }
            _ => {
                return Err(ApiError::bad_request(
                    format!("unknown op \"{op}\""),
                    format!("{base}/op"),
                ));
            }
        };
        changes.push(change);
    }
    Ok(changes)
}

/// Compute the changes that turn `before` into `after`.
fn compute_diff(before: &[Row], after: &[Row]) -> Vec<Change> {
    let before_by_id: HashMap<&str, &Row> = before.iter().map(|row| (row.id.as_str(), row)).collect();
    let after_by_id: HashMap<&str, &Row> = after.iter().map(|row| (row.id.as_str(), row)).collect();

    let mut changes = Vec::new();
    for row in before {
        if !after_by_id.contains_key(row.id.as_str()) {
            changes.push(Change::Remove {
                id: row.id.clone(),
                value: row.values.clone(),
            });
        }
    }
    for row in after {
        match before_by_id.get(row.id.as_str()) {
            None => changes.push(Change::Add {
                id: row.id.clone(),
                value: row.values.clone(),
            }),
            Some(old) => {
                let mut fields = Vec::new();
                diff_objects(&old.values, &row.values, "", &mut fields);
                if !fields.is_empty() {
                    fields.sort_by(|a, b| a.pointer.cmp(&b.pointer));
                    changes.push(Change::Update {
                        id: row.id.clone(),
                        fields,
                    });
                }
            }
        }
    }
    changes.sort_by(|a, b| a.id().cmp(b.id()));
    changes
}

fn diff_objects(
    old: &Map<String, Value>,
    new: &Map<String, Value>,
    prefix: &str,
    out: &mut Vec<FieldChange>,
) {
    for (key, old_value) in old {
        let pointer = format!("{prefix}/{}", escape_token(key));
        match new.get(key) {
            None => out.push(FieldChange {
                pointer,
                segments: Vec::new(),
                old: Some(old_value.clone()),
                new: None,
            }),
            Some(new_value) => match (old_value, new_value) {
                (Value::Object(old_inner), Value::Object(new_inner)) => {
                    diff_objects(old_inner, new_inner, &pointer, out);
                }
                _ if !json_eq(old_value, new_value) => out.push(FieldChange {
                    pointer,
                    segments: Vec::new(),
                    old: Some(old_value.clone()),
                    new: Some(new_value.clone()),
                }),
                _ => {}
            },
        }
    }
    for (key, new_value) in new {
        if !old.contains_key(key) {
            out.push(FieldChange {
                pointer: format!("{prefix}/{}", escape_token(key)),
                segments: Vec::new(),
                old: None,
                new: Some(new_value.clone()),
            });
        }
    }
}

/// Replay `changes` over `rows`, validating everything before mutating so a
/// failure never produces a partially applied result. Rows come back sorted
/// by id.
fn apply_changes(
    rows: Vec<Row>,
    changes: &[Change],
    direction: Direction,
) -> Result<Vec<Row>, ApiError> {
    let mut by_id: BTreeMap<String, Map<String, Value>> =
        rows.into_iter().map(|row| (row.id, row.values)).collect();
    for (i, change) in changes.iter().enumerate() {
        validate_change(&by_id, change, direction, &format!("/changes/{i}"))?;
    }
    for change in changes {
        apply_change(&mut by_id, change, direction);
    }
    Ok(by_id
        .into_iter()
        .map(|(id, values)| Row { id, values })
        .collect())
}

fn validate_change(
    rows: &BTreeMap<String, Map<String, Value>>,
    change: &Change,
    direction: Direction,
    base: &str,
) -> Result<(), ApiError> {
    match change {
        Change::Add { id, value } => match direction {
            Direction::Forward => expect_row_absent(rows, id, base),
            Direction::Reverse => expect_row_present(rows, id, value, base),
        },
        Change::Remove { id, value } => match direction {
            Direction::Forward => expect_row_present(rows, id, value, base),
            Direction::Reverse => expect_row_absent(rows, id, base),
        },
        Change::Update { id, fields } => {
            let values = rows.get(id).ok_or_else(|| {
                ApiError::conflict(format!("row \"{id}\" does not exist"), format!("{base}/id"))
            })?;
            for (j, field) in fields.iter().enumerate() {
                validate_field(values, field, direction, &format!("{base}/fields/{j}"))?;
            }
            Ok(())
        }
    }
}

fn expect_row_absent(
    rows: &BTreeMap<String, Map<String, Value>>,
    id: &str,
    base: &str,
) -> Result<(), ApiError> {
    if rows.contains_key(id) {
        return Err(ApiError::conflict(
            format!("row \"{id}\" already exists"),
            format!("{base}/id"),
        ));
    }
    Ok(())
}

fn expect_row_present(
    rows: &BTreeMap<String, Map<String, Value>>,
    id: &str,
    value: &Map<String, Value>,
    base: &str,
) -> Result<(), ApiError> {
    match rows.get(id) {
        None => Err(ApiError::conflict(
            format!("row \"{id}\" does not exist"),
            format!("{base}/id"),
        )),
        Some(current) if !json_eq(&Value::Object(current.clone()), &Value::Object(value.clone())) => {
            Err(ApiError::conflict(
                format!("values of row \"{id}\" do not match the expected value"),
                format!("{base}/value"),
            ))
        }
        Some(_) => Ok(()),
    }
}

fn validate_field(
    values: &Map<String, Value>,
    field: &FieldChange,
    direction: Direction,
    base: &str,
) -> Result<(), ApiError> {
    let (expected, target, side) = match direction {
        Direction::Forward => (&field.old, &field.new, "old"),
        Direction::Reverse => (&field.new, &field.old, "new"),
    };
    let current = get_path(values, &field.segments);
    match expected {
        Some(want) => match current {
            None => {
                return Err(ApiError::conflict(
                    format!("no value at \"{}\", expected {side} value", field.pointer),
                    format!("{base}/{side}"),
                ));
            }
            Some(got) if !json_eq(got, want) => {
                return Err(ApiError::conflict(
                    format!("value at \"{}\" does not match expected {side} value", field.pointer),
                    format!("{base}/{side}"),
                ));
            }
            Some(_) => {}
        },
        None => {
            if current.is_some() {
                return Err(ApiError::conflict(
                    format!("unexpected value at \"{}\", field should be absent", field.pointer),
                    format!("{base}/{side}"),
                ));
            }
        }
    }
    if target.is_some() {
        // Setting a value walks the intermediate segments; any existing
        // intermediate that is not an object would have to be destroyed.
        let mut cursor = values;
        for segment in &field.segments[..field.segments.len() - 1] {
            match cursor.get(segment) {
                None => break,
                Some(Value::Object(inner)) => cursor = inner,
                Some(_) => {
                    return Err(ApiError::conflict(
                        format!(
                            "cannot set \"{}\": segment \"{segment}\" is not an object",
                            field.pointer
                        ),
                        format!("{base}/path"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn apply_change(
    rows: &mut BTreeMap<String, Map<String, Value>>,
    change: &Change,
    direction: Direction,
) {
    match change {
        Change::Add { id, value } => match direction {
            Direction::Forward => {
                rows.insert(id.clone(), value.clone());
            }
            Direction::Reverse => {
                rows.remove(id);
            }
        },
        Change::Remove { id, value } => match direction {
            Direction::Forward => {
                rows.remove(id);
            }
            Direction::Reverse => {
                rows.insert(id.clone(), value.clone());
            }
        },
        Change::Update { id, fields } => {
            let values = rows.get_mut(id).expect("row existence was validated");
            for field in fields {
                let target = match direction {
                    Direction::Forward => &field.new,
                    Direction::Reverse => &field.old,
                };
                set_path(values, &field.segments, target);
            }
        }
    }
}

fn get_path<'a>(values: &'a Map<String, Value>, segments: &[String]) -> Option<&'a Value> {
    let mut current = values.get(segments.first()?)?;
    for segment in &segments[1..] {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn set_path(values: &mut Map<String, Value>, segments: &[String], target: &Option<Value>) {
    let mut cursor = values;
    for segment in &segments[..segments.len() - 1] {
        cursor = cursor
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("intermediate segments were validated as objects");
    }
    let last = segments.last().expect("paths have at least one segment");
    match target {
        Some(value) => {
            cursor.insert(last.clone(), value.clone());
        }
        None => {
            cursor.remove(last);
        }
    }
}

/// Structural equality with mathematical number comparison; object key order
/// is irrelevant because maps are unordered by construction.
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Number(x), Value::Number(y)) => number_eq(x, y),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(u, v)| json_eq(u, v))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => false,
    }
}

fn number_eq(a: &Number, b: &Number) -> bool {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return x == y;
    }
    a.as_f64() == b.as_f64()
}

fn escape_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn unescape_token(token: &str) -> Result<String, String> {
    let mut out = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(c) = chars.next() {
        if c == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => return Err(format!("invalid escape sequence in pointer segment \"{token}\"")),
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// Parse an RFC 6901 JSON Pointer into unescaped segments. The empty pointer
/// is rejected: field changes always address a member of the row values.
fn parse_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Err("path must not be the empty pointer".to_owned());
    }
    if !pointer.starts_with('/') {
        return Err(format!("path \"{pointer}\" is not a valid RFC 6901 JSON pointer"));
    }
    pointer[1..].split('/').map(unescape_token).collect()
}

fn row_to_json(row: &Row) -> Value {
    json!({ "id": row.id, "values": row.values })
}

fn change_to_json(change: &Change) -> Value {
    match change {
        Change::Add { id, value } => json!({ "id": id, "op": "add", "value": value }),
        Change::Remove { id, value } => json!({ "id": id, "op": "remove", "value": value }),
        Change::Update { id, fields } => {
            let fields: Vec<Value> = fields
                .iter()
                .map(|field| {
                    let mut object = Map::new();
                    object.insert("path".to_owned(), Value::String(field.pointer.clone()));
                    if let Some(old) = &field.old {
                        object.insert("old".to_owned(), old.clone());
                    }
                    if let Some(new) = &field.new {
                        object.insert("new".to_owned(), new.clone());
                    }
                    Value::Object(object)
                })
                .collect();
            json!({ "id": id, "op": "update", "fields": fields })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows_json(rows: &[(&str, Value)]) -> Value {
        Value::Array(
            rows.iter()
                .map(|(id, values)| json!({ "id": id, "values": values }))
                .collect(),
        )
    }

    fn diff(before: Value, after: Value) -> Value {
        diff_endpoint(
            json!({ "before": before, "after": after })
                .to_string()
                .as_bytes(),
        )
        .expect("diff should succeed")
    }

    fn apply(rows: Value, changes: Value, direction: &str) -> Result<Value, ApiError> {
        apply_endpoint(
            json!({ "rows": rows, "changes": changes, "direction": direction })
                .to_string()
                .as_bytes(),
        )
    }

    #[test]
    fn diff_add_remove_update() {
        let before = rows_json(&[
            ("a", json!({ "x": 1 })),
            ("b", json!({ "y": 2 })),
            ("c", json!({ "z": 3 })),
        ]);
        let after = rows_json(&[
            ("b", json!({ "y": 4 })),
            ("c", json!({ "z": 3 })),
            ("d", json!({ "w": 5 })),
        ]);
        let result = diff(before, after);
        assert_eq!(
            result["changes"],
            json!([
                { "id": "a", "op": "remove", "value": { "x": 1 } },
                { "id": "b", "op": "update", "fields": [{ "path": "/y", "old": 2, "new": 4 }] },
                { "id": "d", "op": "add", "value": { "w": 5 } },
            ])
        );
    }

    #[test]
    fn diff_nested_and_missing_fields() {
        let before = rows_json(&[("r", json!({ "a": { "b": 1, "c": 2 }, "gone": true }))]);
        let after = rows_json(&[("r", json!({ "a": { "b": 9 }, "new": null }))]);
        let result = diff(before, after);
        assert_eq!(
            result["changes"],
            json!([{
                "id": "r",
                "op": "update",
                "fields": [
                    { "path": "/a/b", "old": 1, "new": 9 },
                    { "path": "/a/c", "old": 2 },
                    { "path": "/gone", "old": true },
                    { "path": "/new", "new": null },
                ],
            }])
        );
    }

    #[test]
    fn diff_numbers_compare_mathematically_and_keys_unordered() {
        let before = rows_json(&[("r", json!({ "n": 1, "o": { "a": 1, "b": 2 } }))]);
        let after = rows_json(&[("r", json!({ "n": 1.0, "o": { "b": 2, "a": 1 } }))]);
        assert_eq!(diff(before, after)["changes"], json!([]));
    }

    #[test]
    fn diff_arrays_compare_as_whole() {
        let before = rows_json(&[("r", json!({ "list": [1, 2] }))]);
        let after = rows_json(&[("r", json!({ "list": [1, 2, 3] }))]);
        let result = diff(before, after);
        assert_eq!(
            result["changes"],
            json!([{
                "id": "r",
                "op": "update",
                "fields": [{ "path": "/list", "old": [1, 2], "new": [1, 2, 3] }],
            }])
        );
    }

    #[test]
    fn diff_escapes_pointer_tokens() {
        let before = rows_json(&[("r", json!({ "a/b~c": 1 }))]);
        let after = rows_json(&[("r", json!({ "a/b~c": 2 }))]);
        let result = diff(before, after);
        assert_eq!(
            result["changes"][0]["fields"][0]["path"],
            json!("/a~1b~0c")
        );
    }

    #[test]
    fn diff_rejects_duplicate_ids() {
        let dup = json!([
            { "id": "a", "values": {} },
            { "id": "a", "values": {} },
        ]);
        let err = diff_endpoint(
            json!({ "before": dup, "after": [] }).to_string().as_bytes(),
        )
        .unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/before/1/id");
    }

    #[test]
    fn apply_forward_and_reverse_roundtrip() {
        let before = rows_json(&[
            ("a", json!({ "x": 1 })),
            ("b", json!({ "n": { "deep": "v" } })),
        ]);
        let after = rows_json(&[
            ("b", json!({ "n": { "deep": "w", "extra": null } })),
            ("c", json!({ "fresh": true })),
        ]);
        let changes = diff(before.clone(), after.clone())["changes"].clone();

        let forward = apply(before.clone(), changes.clone(), "forward").expect("forward apply");
        assert_eq!(forward["rows"], after);

        let reverse = apply(after.clone(), changes, "reverse").expect("reverse apply");
        assert_eq!(reverse["rows"], before);
    }

    #[test]
    fn apply_sorts_rows_by_id() {
        let rows = rows_json(&[("b", json!({})), ("a", json!({}))]);
        let result = apply(rows, json!([]), "forward").expect("apply");
        let ids: Vec<&str> = result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["a", "b"]);
    }

    #[test]
    fn apply_conflict_on_old_value_mismatch() {
        let rows = rows_json(&[("a", json!({ "x": 2 }))]);
        let changes = json!([{ "id": "a", "op": "update", "fields": [{ "path": "/x", "old": 1, "new": 3 }] }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/fields/0/old");
    }

    #[test]
    fn apply_distinguishes_missing_from_null() {
        // old: null expects an explicit null, not an absent key.
        let rows = rows_json(&[("a", json!({}))]);
        let changes = json!([{ "id": "a", "op": "update", "fields": [{ "path": "/x", "old": null, "new": 1 }] }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);

        // absent old expects the key to be missing; an explicit null conflicts.
        let rows = rows_json(&[("a", json!({ "x": null }))]);
        let changes = json!([{ "id": "a", "op": "update", "fields": [{ "path": "/x", "new": 1 }] }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);
    }

    #[test]
    fn apply_conflict_on_row_existence() {
        let rows = rows_json(&[("a", json!({ "x": 1 }))]);
        let changes = json!([{ "id": "a", "op": "add", "value": { "x": 1 } }]);
        let err = apply(rows.clone(), changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);

        let changes = json!([{ "id": "ghost", "op": "remove", "value": {} }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);
    }

    #[test]
    fn apply_remove_checks_expected_value() {
        let rows = rows_json(&[("a", json!({ "x": 1 }))]);
        let changes = json!([{ "id": "a", "op": "remove", "value": { "x": 2 } }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/value");
    }

    #[test]
    fn apply_rejects_duplicate_paths() {
        let rows = rows_json(&[("a", json!({ "x": 1 }))]);
        let changes = json!([{
            "id": "a",
            "op": "update",
            "fields": [
                { "path": "/x", "old": 1, "new": 2 },
                { "path": "/x", "old": 1, "new": 3 },
            ],
        }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/changes/0/fields/1/path");
    }

    #[test]
    fn apply_rejects_bad_direction_and_pointer() {
        let rows = rows_json(&[("a", json!({}))]);
        let err = apply(rows.clone(), json!([]), "sideways").unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/direction");

        let changes = json!([{ "id": "a", "op": "update", "fields": [{ "path": "x", "new": 1 }] }]);
        let err = apply(rows, changes, "forward").unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/changes/0/fields/0/path");
    }

    #[test]
    fn apply_reverse_of_update_restores_old() {
        let rows = rows_json(&[("a", json!({ "k": { "m": 5 }, "drop": 1 }))]);
        let changes = json!([{
            "id": "a",
            "op": "update",
            "fields": [
                { "path": "/k/m", "old": 5, "new": 6 },
                { "path": "/drop", "old": 1 },
                { "path": "/added", "new": "hi" },
            ],
        }]);
        let forward = apply(rows.clone(), changes.clone(), "forward").expect("forward");
        assert_eq!(
            forward["rows"],
            json!([{ "id": "a", "values": { "k": { "m": 6 }, "added": "hi" } }])
        );
        let back = apply(forward["rows"].clone(), changes, "reverse").expect("reverse");
        assert_eq!(back["rows"], rows);
    }
}
