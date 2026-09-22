//! 无状态快照差异（diff）与双向回放（apply）的核心逻辑。
//!
//! 本模块只接收和返回 `serde_json::Value`，不感知 HTTP；
//! HTTP 层仅负责 JSON 解析与状态码映射。

use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;

/// 业务错误。`status` 为 HTTP 状态码，`path` 用 RFC 6901 风格定位请求体中的出错字段。
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
    pub path: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            status: 400,
            code: "bad_request",
            message: message.into(),
            path: path.into(),
        }
    }

    pub fn conflict(message: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            status: 409,
            code: "conflict",
            message: message.into(),
            path: path.into(),
        }
    }
}

/// 对象字段差异。`old`/`new` 为 `None` 表示该侧字段缺失（与 JSON null 严格区分）。
#[derive(Debug, Clone, PartialEq)]
struct Field {
    path: String,
    old: Option<Value>,
    new: Option<Value>,
}

#[derive(Debug)]
enum Change {
    Add { id: String, value: Value },
    Remove { id: String, value: Value },
    Update { id: String, fields: Vec<Field> },
}

impl Change {
    fn id(&self) -> &str {
        match self {
            Change::Add { id, .. } | Change::Remove { id, .. } | Change::Update { id, .. } => id,
        }
    }
}

/// 计算 before → after 的差异，返回 `{"changes": [...]}`。
pub fn diff(request: &Value) -> Result<Value, ApiError> {
    let obj = request_object(request)?;
    let before = parse_rows(obj.get("before"), "/before")?;
    let after = parse_rows(obj.get("after"), "/after")?;

    // before / after 均已按 id 升序，归并遍历即得按 id 升序的 changes。
    let mut changes = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < before.len() || j < after.len() {
        match (before.get(i), after.get(j)) {
            (Some(b), Some(a)) => match b.0.cmp(&a.0) {
                std::cmp::Ordering::Less => {
                    changes.push(row_change(&b.0, "remove", &b.1));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    changes.push(row_change(&a.0, "add", &a.1));
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    let mut fields = Vec::new();
                    diff_value("", Some(&b.1), Some(&a.1), &mut fields);
                    if !fields.is_empty() {
                        fields.sort_by(|x, y| x.path.cmp(&y.path));
                        changes.push(update_change(&a.0, &fields));
                    }
                    i += 1;
                    j += 1;
                }
            },
            (Some(b), None) => {
                changes.push(row_change(&b.0, "remove", &b.1));
                i += 1;
            }
            (None, Some(a)) => {
                changes.push(row_change(&a.0, "add", &a.1));
                j += 1;
            }
            (None, None) => break,
        }
    }

    let mut root = Map::new();
    root.insert("changes".to_owned(), Value::Array(changes));
    Ok(Value::Object(root))
}

/// 校验并应用变更，返回按 id 升序的 `{"rows": [...]}`。
/// 正向（forward）应用 old→new，反向（reverse）还原 new→old。
/// 任一校验失败即整体拒绝，不部分应用。
pub fn apply(request: &Value) -> Result<Value, ApiError> {
    let obj = request_object(request)?;
    let rows = parse_rows(obj.get("rows"), "/rows")?;

    let direction_path = "/direction";
    let direction = obj
        .get("direction")
        .and_then(Value::as_str)
        .filter(|d| *d == "forward" || *d == "reverse")
        .ok_or_else(|| {
            ApiError::bad_request(
                "direction must be \"forward\" or \"reverse\"",
                direction_path,
            )
        })?;
    let reverse = direction == "reverse";

    let changes_value = obj
        .get("changes")
        .ok_or_else(|| ApiError::bad_request("missing required field", "/changes"))?;
    let changes_array = changes_value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("changes must be an array", "/changes"))?;
    let mut changes = Vec::with_capacity(changes_array.len());
    for (i, item) in changes_array.iter().enumerate() {
        changes.push(parse_change(item, &format!("/changes/{i}"))?);
    }
    for (i, change) in changes.iter().enumerate() {
        if changes[..i].iter().any(|c| c.id() == change.id()) {
            return Err(ApiError::bad_request(
                format!("duplicate change for row id {:?}", change.id()),
                format!("/changes/{i}/id"),
            ));
        }
    }

    // 每个 change 作用于不同的行，因此可以全部基于原始状态校验后再统一应用。
    let mut rows_map: BTreeMap<String, Value> = rows.into_iter().collect();
    for (i, change) in changes.iter().enumerate() {
        validate_change(&rows_map, change, reverse, &format!("/changes/{i}"))?;
    }
    for change in &changes {
        apply_change(&mut rows_map, change, reverse);
    }

    let rows_json = rows_map
        .into_iter()
        .map(|(id, values)| {
            let mut m = Map::new();
            m.insert("id".to_owned(), Value::String(id));
            m.insert("values".to_owned(), values);
            Value::Object(m)
        })
        .collect();
    let mut root = Map::new();
    root.insert("rows".to_owned(), Value::Array(rows_json));
    Ok(Value::Object(root))
}

fn request_object(request: &Value) -> Result<&Map<String, Value>, ApiError> {
    request
        .as_object()
        .ok_or_else(|| ApiError::bad_request("request body must be a JSON object", ""))
}

/// 解析并校验行数组：id 为非空唯一字符串，values 为对象。返回按 id 升序的行。
fn parse_rows(value: Option<&Value>, path: &str) -> Result<Vec<(String, Value)>, ApiError> {
    let value = value.ok_or_else(|| ApiError::bad_request("missing required field", path))?;
    let array = value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("expected an array of rows", path))?;

    let mut rows = Vec::with_capacity(array.len());
    for (i, item) in array.iter().enumerate() {
        let item_path = format!("{path}/{i}");
        let obj = item
            .as_object()
            .ok_or_else(|| ApiError::bad_request("row must be an object", &item_path))?;
        let id_path = format!("{item_path}/id");
        let id = obj
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("row id must be a string", &id_path))?;
        if id.is_empty() {
            return Err(ApiError::bad_request("row id must be non-empty", &id_path));
        }
        let values_path = format!("{item_path}/values");
        let values = obj
            .get("values")
            .ok_or_else(|| ApiError::bad_request("missing required field", &values_path))?;
        if !values.is_object() {
            return Err(ApiError::bad_request(
                "row values must be an object",
                &values_path,
            ));
        }
        rows.push((id.to_owned(), values.clone()));
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in rows.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(ApiError::bad_request(
                format!("duplicate row id {:?}", pair[0].0),
                path,
            ));
        }
    }
    Ok(rows)
}

fn parse_change(value: &Value, path: &str) -> Result<Change, ApiError> {
    let obj = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("change must be an object", path))?;

    let id_path = format!("{path}/id");
    let id = obj
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("change id must be a string", &id_path))?;
    if id.is_empty() {
        return Err(ApiError::bad_request("change id must be non-empty", &id_path));
    }

    let op_path = format!("{path}/op");
    let op = obj
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("change op must be a string", &op_path))?;

    match op {
        "add" | "remove" => {
            let value_path = format!("{path}/value");
            let row_value = obj
                .get("value")
                .ok_or_else(|| ApiError::bad_request("missing required field", &value_path))?;
            if !row_value.is_object() {
                return Err(ApiError::bad_request(
                    "change value must be an object",
                    &value_path,
                ));
            }
            let id = id.to_owned();
            let value = row_value.clone();
            Ok(if op == "add" {
                Change::Add { id, value }
            } else {
                Change::Remove { id, value }
            })
        }
        "update" => {
            let fields_path = format!("{path}/fields");
            let fields_value = obj
                .get("fields")
                .ok_or_else(|| ApiError::bad_request("missing required field", &fields_path))?;
            let fields_array = fields_value
                .as_array()
                .ok_or_else(|| ApiError::bad_request("fields must be an array", &fields_path))?;

            let mut fields = Vec::with_capacity(fields_array.len());
            for (i, item) in fields_array.iter().enumerate() {
                fields.push(parse_field(item, &format!("{fields_path}/{i}"))?);
            }
            for (i, field) in fields.iter().enumerate() {
                if fields[..i].iter().any(|f| f.path == field.path) {
                    return Err(ApiError::bad_request(
                        format!("duplicate field path {:?}", field.path),
                        format!("{fields_path}/{i}/path"),
                    ));
                }
            }
            Ok(Change::Update {
                id: id.to_owned(),
                fields,
            })
        }
        _ => Err(ApiError::bad_request(
            format!("unknown change op {op:?}"),
            &op_path,
        )),
    }
}

fn parse_field(value: &Value, path: &str) -> Result<Field, ApiError> {
    let obj = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("field must be an object", path))?;

    let pointer_path = format!("{path}/path");
    let pointer = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("field path must be a string", &pointer_path))?;
    let segments = parse_pointer(pointer).ok_or_else(|| {
        ApiError::bad_request(
            "field path must be a non-empty RFC 6901 JSON Pointer",
            &pointer_path,
        )
    })?;

    let old = obj.get("old").cloned();
    let new = obj.get("new").cloned();
    if old.is_none() && new.is_none() {
        return Err(ApiError::bad_request(
            "field must contain old and/or new",
            path,
        ));
    }
    // 归一化为规范转义形式，保证路径唯一性比较不受等价写法影响。
    Ok(Field {
        path: canonical_pointer(&segments),
        old,
        new,
    })
}

/// 递归比较两个值，把差异追加到 `out`。数组与非对象值整体比较。
fn diff_value(path: &str, old: Option<&Value>, new: Option<&Value>, out: &mut Vec<Field>) {
    match (old, new) {
        (Some(Value::Object(old_obj)), Some(Value::Object(new_obj))) => {
            for (key, old_value) in old_obj {
                let child_path = format!("{path}/{}", escape_segment(key));
                match new_obj.get(key) {
                    Some(new_value) => {
                        diff_value(&child_path, Some(old_value), Some(new_value), out)
                    }
                    None => out.push(Field {
                        path: child_path,
                        old: Some(old_value.clone()),
                        new: None,
                    }),
                }
            }
            for (key, new_value) in new_obj {
                if !old_obj.contains_key(key) {
                    out.push(Field {
                        path: format!("{path}/{}", escape_segment(key)),
                        old: None,
                        new: Some(new_value.clone()),
                    });
                }
            }
        }
        _ => {
            let equal = match (old, new) {
                (Some(a), Some(b)) => json_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if !equal {
                out.push(Field {
                    path: path.to_owned(),
                    old: old.cloned(),
                    new: new.cloned(),
                });
            }
        }
    }
}

/// 递归相等：对象键序无关，数字按数学值比较（如 1 与 1.0 相等）。
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => number_eq(x, y),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(u, v)| json_eq(u, v))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => a == b,
    }
}

/// JSON 数字的无损数学值：符号、归一化有效数字、10 进指数（无符号十进制串）。
/// 相同数值的不同写法（1、1.0、1e0、100e-2）归一化后完全一致。
/// 指数用字符串保存，避免任何宽度的整数溢出。
#[derive(Debug, PartialEq, Eq)]
struct DecimalValue {
    negative: bool,
    digits: String,
    exp_negative: bool,
    exp_magnitude: String,
}

/// 无损十进制比较：直接解析数字词法，全程不经过 f64。
/// 依赖 serde_json 的 arbitrary_precision，Number 保留输入的原始词法。
fn number_eq(a: &Number, b: &Number) -> bool {
    decimal_value(a.as_str()) == decimal_value(b.as_str())
}

fn decimal_value(lexeme: &str) -> DecimalValue {
    let bytes = lexeme.as_bytes();
    let mut pos = 0;
    let negative = bytes.first() == Some(&b'-');
    if negative || bytes.first() == Some(&b'+') {
        pos = 1;
    }

    let mut digits = String::with_capacity(lexeme.len());
    let mut int_digits: Option<usize> = None;
    while pos < bytes.len() {
        match bytes[pos] {
            b'0'..=b'9' => digits.push(bytes[pos] as char),
            b'.' => int_digits = Some(digits.len()),
            b'e' | b'E' => {
                pos += 1;
                break;
            }
            _ => {}
        }
        pos += 1;
    }
    // 小数点后的位数用于把尾数折算成整数（请求体大小内，必然适配 i64）。
    let frac_digits = int_digits
        .map(|n| digits.len() as i64 - n as i64)
        .unwrap_or(0);

    // 解析指数部分。
    let mut exp_neg = false;
    let mut exp_raw = String::new();
    if pos < bytes.len() && (bytes[pos] == b'-' || bytes[pos] == b'+') {
        exp_neg = bytes[pos] == b'-';
        pos += 1;
    }
    while pos < bytes.len() {
        if bytes[pos].is_ascii_digit() {
            exp_raw.push(bytes[pos] as char);
        }
        pos += 1;
    }
    let mut exp_magnitude = normalize_magnitude(&exp_raw);
    if exp_magnitude == "0" {
        exp_neg = false;
    }
    // 值 = 有效整数 × 10^(指数 − 小数位数)。
    (exp_neg, exp_magnitude) = exp_adjust(exp_neg, &exp_magnitude, -frac_digits);

    // 去除前导零。
    let start = digits
        .bytes()
        .position(|c| c != b'0')
        .unwrap_or(digits.len());
    if start == digits.len() {
        // 0、-0、0.0、0e10 等一律归一化为正零。
        return DecimalValue {
            negative: false,
            digits: "0".to_owned(),
            exp_negative: false,
            exp_magnitude: "0".to_owned(),
        };
    }
    digits.replace_range(..start, "");

    // 去除末尾零并相应抬高指数，数值保持不变；归一化后的尾数首尾均非零，
    // 因此（符号、尾数、指数）三元组与数学值一一对应。
    let trailing = digits.len() - digits.trim_end_matches('0').len();
    digits.truncate(digits.len() - trailing);
    (exp_neg, exp_magnitude) = exp_adjust(exp_neg, &exp_magnitude, trailing as i64);

    DecimalValue {
        negative,
        digits,
        exp_negative: exp_neg,
        exp_magnitude,
    }
}

fn normalize_magnitude(s: &str) -> String {
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn magnitude_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// 无符号十进制整数相加（入参均已去除前导零）。
fn magnitude_add(a: &str, b: &str) -> String {
    let (longer, shorter) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    let mut out = String::with_capacity(longer.len() + 1);
    let mut carry = 0u8;
    let mut lb = longer.as_bytes().iter().rev();
    for sb in shorter.as_bytes().iter().rev() {
        let sum = (sb - b'0') + (lb.next().unwrap() - b'0') + carry;
        out.push((b'0' + sum % 10) as char);
        carry = sum / 10;
    }
    for &c in lb {
        let sum = (c - b'0') + carry;
        out.push((b'0' + sum % 10) as char);
        carry = sum / 10;
    }
    if carry > 0 {
        out.push((b'0' + carry) as char);
    }
    out.chars().rev().collect()
}

/// 无符号十进制整数相减，要求 a >= b。
fn magnitude_sub(a: &str, b: &str) -> String {
    let mut out = String::with_capacity(a.len());
    let mut borrow = 0i16;
    let mut ia = a.as_bytes().iter().rev();
    for sb in b.as_bytes().iter().rev() {
        let digit = (ia.next().unwrap() - b'0') as i16 - (sb - b'0') as i16 - borrow;
        let (digit, new_borrow) = if digit < 0 {
            (digit + 10, 1)
        } else {
            (digit, 0)
        };
        out.push((b'0' + digit as u8) as char);
        borrow = new_borrow;
    }
    for &c in ia {
        let digit = (c - b'0') as i16 - borrow;
        let (digit, new_borrow) = if digit < 0 {
            (digit + 10, 1)
        } else {
            (digit, 0)
        };
        out.push((b'0' + digit as u8) as char);
        borrow = new_borrow;
    }
    let result: String = out.chars().rev().collect();
    normalize_magnitude(&result)
}

/// 带符号十进制大整数（符号 + 无符号十进制串）加上一个 i64 小量。
fn exp_adjust(neg: bool, magnitude: &str, delta: i64) -> (bool, String) {
    if magnitude == "0" {
        if delta == 0 {
            (false, "0".to_owned())
        } else {
            (
                delta < 0,
                normalize_magnitude(&delta.unsigned_abs().to_string()),
            )
        }
    } else if delta == 0 {
        (neg, magnitude.to_owned())
    } else {
        let d_magnitude = normalize_magnitude(&delta.unsigned_abs().to_string());
        let d_neg = delta < 0;
        if neg == d_neg {
            (neg, magnitude_add(magnitude, &d_magnitude))
        } else {
            match magnitude_cmp(magnitude, &d_magnitude) {
                std::cmp::Ordering::Greater => (neg, magnitude_sub(magnitude, &d_magnitude)),
                std::cmp::Ordering::Less => (d_neg, magnitude_sub(&d_magnitude, magnitude)),
                std::cmp::Ordering::Equal => (false, "0".to_owned()),
            }
        }
    }
}

fn escape_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// 解析 RFC 6901 JSON Pointer。空指针（指向整个文档）在此不允许，返回 None。
fn parse_pointer(path: &str) -> Option<Vec<String>> {
    if path.is_empty() || !path.starts_with('/') {
        return None;
    }
    let mut segments = Vec::new();
    for raw in path[1..].split('/') {
        let mut segment = String::with_capacity(raw.len());
        let mut chars = raw.chars();
        while let Some(c) = chars.next() {
            if c == '~' {
                match chars.next() {
                    Some('0') => segment.push('~'),
                    Some('1') => segment.push('/'),
                    _ => return None,
                }
            } else {
                segment.push(c);
            }
        }
        segments.push(segment);
    }
    Some(segments)
}

fn canonical_pointer(segments: &[String]) -> String {
    let mut out = String::new();
    for segment in segments {
        out.push('/');
        out.push_str(&escape_segment(segment));
    }
    out
}

fn get_path<'a>(root: &'a Value, segments: &[String]) -> Option<&'a Value> {
    let mut current = root;
    for segment in segments {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

/// 除最后一段外，路径沿途必须都是已存在的对象。
fn parent_is_object(root: &Value, segments: &[String]) -> bool {
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        match current.as_object().and_then(|m| m.get(segment)) {
            Some(next) if next.is_object() => current = next,
            _ => return false,
        }
    }
    true
}

fn set_path(root: &mut Value, segments: &[String], value: Value) {
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        current = match current.as_object_mut().and_then(|m| m.get_mut(segment)) {
            Some(next) if next.is_object() => next,
            _ => unreachable!("parent path validated before apply"),
        };
    }
    if let Some(obj) = current.as_object_mut() {
        obj.insert(segments[segments.len() - 1].clone(), value);
    }
}

fn remove_path(root: &mut Value, segments: &[String]) {
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        current = match current.as_object_mut().and_then(|m| m.get_mut(segment)) {
            Some(next) if next.is_object() => next,
            _ => return,
        };
    }
    if let Some(obj) = current.as_object_mut() {
        obj.remove(&segments[segments.len() - 1]);
    }
}

fn row_change(id: &str, op: &str, value: &Value) -> Value {
    let mut m = Map::new();
    m.insert("id".to_owned(), Value::String(id.to_owned()));
    m.insert("op".to_owned(), Value::String(op.to_owned()));
    m.insert("value".to_owned(), value.clone());
    Value::Object(m)
}

fn update_change(id: &str, fields: &[Field]) -> Value {
    let mut m = Map::new();
    m.insert("id".to_owned(), Value::String(id.to_owned()));
    m.insert("op".to_owned(), Value::String("update".to_owned()));
    m.insert(
        "fields".to_owned(),
        Value::Array(fields.iter().map(field_json).collect()),
    );
    Value::Object(m)
}

fn field_json(field: &Field) -> Value {
    let mut m = Map::new();
    m.insert("path".to_owned(), Value::String(field.path.clone()));
    if let Some(old) = &field.old {
        m.insert("old".to_owned(), old.clone());
    }
    if let Some(new) = &field.new {
        m.insert("new".to_owned(), new.clone());
    }
    Value::Object(m)
}

fn validate_change(
    rows: &BTreeMap<String, Value>,
    change: &Change,
    reverse: bool,
    change_path: &str,
) -> Result<(), ApiError> {
    match change {
        // 正向 add：行必须不存在；反向 add：行必须存在且内容等于 value。
        Change::Add { id, value } => {
            if reverse {
                expect_row(rows, id, value, change_path)
            } else if rows.contains_key(id) {
                Err(ApiError::conflict(
                    format!("row {id:?} already exists"),
                    format!("{change_path}/id"),
                ))
            } else {
                Ok(())
            }
        }
        // 正向 remove：行必须存在且内容等于 value；反向 remove：行必须不存在。
        Change::Remove { id, value } => {
            if reverse {
                if rows.contains_key(id) {
                    Err(ApiError::conflict(
                        format!("row {id:?} already exists"),
                        format!("{change_path}/id"),
                    ))
                } else {
                    Ok(())
                }
            } else {
                expect_row(rows, id, value, change_path)
            }
        }
        Change::Update { id, fields } => {
            let row = rows.get(id).ok_or_else(|| {
                ApiError::conflict(
                    format!("row {id:?} does not exist"),
                    format!("{change_path}/id"),
                )
            })?;
            for (i, field) in fields.iter().enumerate() {
                validate_field(row, field, reverse, &format!("{change_path}/fields/{i}"))?;
            }
            Ok(())
        }
    }
}

fn expect_row(
    rows: &BTreeMap<String, Value>,
    id: &str,
    value: &Value,
    change_path: &str,
) -> Result<(), ApiError> {
    match rows.get(id) {
        None => Err(ApiError::conflict(
            format!("row {id:?} does not exist"),
            format!("{change_path}/id"),
        )),
        Some(current) if !json_eq(current, value) => Err(ApiError::conflict(
            format!("row {id:?} values do not match expected value"),
            format!("{change_path}/value"),
        )),
        _ => Ok(()),
    }
}

fn validate_field(
    row: &Value,
    field: &Field,
    reverse: bool,
    field_path: &str,
) -> Result<(), ApiError> {
    let segments = parse_pointer(&field.path).expect("field path validated at parse time");
    // 校验的一侧：正向看 old，反向看 new。None 表示该路径必须缺失（与 null 区分）。
    let expected = if reverse { &field.new } else { &field.old };
    let current = get_path(row, &segments);
    let side = if reverse { "new" } else { "old" };
    match (expected, current) {
        (None, None) => {}
        (Some(e), Some(c)) if json_eq(e, c) => {}
        (None, Some(_)) => {
            return Err(ApiError::conflict(
                "expected field to be absent but it exists",
                field_path.to_owned(),
            ));
        }
        (Some(_), None) => {
            return Err(ApiError::conflict(
                "expected value but field is missing",
                format!("{field_path}/{side}"),
            ));
        }
        (Some(_), Some(_)) => {
            return Err(ApiError::conflict(
                "field value does not match expected value",
                format!("{field_path}/{side}"),
            ));
        }
    }
    // 写入的一侧需要父路径存在且为对象。
    let target = if reverse { &field.old } else { &field.new };
    if target.is_some() && !parent_is_object(row, &segments) {
        return Err(ApiError::conflict(
            "parent path does not exist or is not an object",
            format!("{field_path}/path"),
        ));
    }
    Ok(())
}

fn apply_change(rows: &mut BTreeMap<String, Value>, change: &Change, reverse: bool) {
    match change {
        Change::Add { id, value } => {
            if reverse {
                rows.remove(id);
            } else {
                rows.insert(id.clone(), value.clone());
            }
        }
        Change::Remove { id, value } => {
            if reverse {
                rows.insert(id.clone(), value.clone());
            } else {
                rows.remove(id);
            }
        }
        Change::Update { id, fields } => {
            let row = rows.get_mut(id).expect("row existence validated before apply");
            for field in fields {
                let segments =
                    parse_pointer(&field.path).expect("field path validated at parse time");
                let target = if reverse { &field.old } else { &field.new };
                match target {
                    Some(value) => set_path(row, &segments, value.clone()),
                    None => remove_path(row, &segments),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn diff_ok(request: Value) -> Value {
        diff(&request).expect("diff should succeed")
    }

    fn apply_ok(request: Value) -> Value {
        apply(&request).expect("apply should succeed")
    }

    #[test]
    fn diff_add_remove_update() {
        let result = diff_ok(json!({
            "before": [
                {"id": "b", "values": {"x": 1}},
                {"id": "c", "values": {"keep": true}}
            ],
            "after": [
                {"id": "a", "values": {"new": 1}},
                {"id": "b", "values": {"x": 2}}
            ]
        }));
        assert_eq!(
            result,
            json!({"changes": [
                {"id": "a", "op": "add", "value": {"new": 1}},
                {"id": "b", "op": "update", "fields": [{"path": "/x", "old": 1, "new": 2}]},
                {"id": "c", "op": "remove", "value": {"keep": true}}
            ]})
        );
    }

    #[test]
    fn diff_ignores_key_order_and_number_form() {
        let result = diff_ok(json!({
            "before": [{"id": "r", "values": {"a": 1, "b": [1, 2.0]}}],
            "after": [{"id": "r", "values": {"b": [1.0, 2], "a": 1.0}}]
        }));
        assert_eq!(result, json!({"changes": []}));
    }

    #[test]
    fn diff_nested_and_missing_vs_null() {
        let result = diff_ok(json!({
            "before": [{"id": "r", "values": {"obj": {"gone": 1, "n": null}, "arr": [1, 2]}}],
            "after": [{"id": "r", "values": {"obj": {"n": null, "added": {"k": "v"}}, "arr": [1, 3]}}]
        }));
        assert_eq!(
            result,
            json!({"changes": [{
                "id": "r",
                "op": "update",
                "fields": [
                    {"path": "/arr", "old": [1, 2], "new": [1, 3]},
                    {"path": "/obj/added", "new": {"k": "v"}},
                    {"path": "/obj/gone", "old": 1}
                ]
            }]})
        );
    }

    #[test]
    fn diff_escapes_pointer_segments() {
        let result = diff_ok(json!({
            "before": [{"id": "r", "values": {"a/b~c": 1}}],
            "after": [{"id": "r", "values": {"a/b~c": 2}}]
        }));
        assert_eq!(
            result,
            json!({"changes": [{
                "id": "r",
                "op": "update",
                "fields": [{"path": "/a~1b~0c", "old": 1, "new": 2}]
            }]})
        );
    }

    #[test]
    fn diff_rejects_duplicate_ids() {
        let err = diff(&json!({
            "before": [{"id": "r", "values": {}}, {"id": "r", "values": {}}],
            "after": []
        }))
        .unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/before");
    }

    #[test]
    fn diff_rejects_non_object_values() {
        let err = diff(&json!({
            "before": [{"id": "r", "values": [1]}],
            "after": []
        }))
        .unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/before/0/values");
    }

    #[test]
    fn apply_forward_then_reverse_roundtrip() {
        let rows = json!([
            {"id": "b", "values": {"x": 1, "deep": {"gone": true}}},
            {"id": "c", "values": {"keep": true}}
        ]);
        let changes = json!([
            {"id": "a", "op": "add", "value": {"new": 1}},
            {"id": "b", "op": "update", "fields": [
                {"path": "/x", "old": 1, "new": 2},
                {"path": "/deep/gone", "old": true},
                {"path": "/deep/added", "new": null}
            ]},
            {"id": "c", "op": "remove", "value": {"keep": true}}
        ]);

        let forward = apply_ok(json!({
            "rows": rows,
            "changes": changes,
            "direction": "forward"
        }));
        let after_rows = json!([
            {"id": "a", "values": {"new": 1}},
            {"id": "b", "values": {"x": 2, "deep": {"added": null}}}
        ]);
        assert_eq!(forward, json!({"rows": after_rows}));

        let backward = apply_ok(json!({
            "rows": after_rows,
            "changes": changes,
            "direction": "reverse"
        }));
        assert_eq!(backward, json!({"rows": rows}));
    }

    #[test]
    fn apply_conflict_on_value_mismatch() {
        let err = apply(&json!({
            "rows": [{"id": "r", "values": {"x": 9}}],
            "changes": [{"id": "r", "op": "update", "fields": [{"path": "/x", "old": 1, "new": 2}]}],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/fields/0/old");
    }

    #[test]
    fn apply_distinguishes_missing_from_null() {
        // 期望缺失，但实际为 null → 409。
        let err = apply(&json!({
            "rows": [{"id": "r", "values": {"x": null}}],
            "changes": [{"id": "r", "op": "update", "fields": [{"path": "/x", "new": 1}]}],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);

        // 期望 null，实际缺失 → 409。
        let err = apply(&json!({
            "rows": [{"id": "r", "values": {}}],
            "changes": [{"id": "r", "op": "update", "fields": [{"path": "/x", "old": null}]}],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);
    }

    #[test]
    fn apply_is_atomic_on_conflict() {
        // 第二个 change 冲突时，第一个 change 也不应生效（整体返回错误）。
        let err = apply(&json!({
            "rows": [{"id": "a", "values": {}}, {"id": "b", "values": {"x": 1}}],
            "changes": [
                {"id": "a", "op": "update", "fields": [{"path": "/y", "new": 1}]},
                {"id": "b", "op": "update", "fields": [{"path": "/x", "old": 2, "new": 3}]}
            ],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/1/fields/0/old");
    }

    #[test]
    fn apply_rejects_bad_direction_and_duplicate_paths() {
        let err = apply(&json!({
            "rows": [],
            "changes": [],
            "direction": "sideways"
        }))
        .unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/direction");

        let err = apply(&json!({
            "rows": [{"id": "r", "values": {"x": 1}}],
            "changes": [{"id": "r", "op": "update", "fields": [
                {"path": "/x", "old": 1, "new": 2},
                {"path": "/x", "old": 1, "new": 3}
            ]}],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.path, "/changes/0/fields/1/path");
    }

    #[test]
    fn apply_reverse_checks_new_side() {
        let err = apply(&json!({
            "rows": [{"id": "r", "values": {"x": 1}}],
            "changes": [{"id": "r", "op": "update", "fields": [{"path": "/x", "old": 1, "new": 2}]}],
            "direction": "reverse"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/fields/0/new");
    }

    #[test]
    fn apply_remove_checks_expected_value() {
        let err = apply(&json!({
            "rows": [{"id": "r", "values": {"x": 1}}],
            "changes": [{"id": "r", "op": "remove", "value": {"x": 2}}],
            "direction": "forward"
        }))
        .unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/value");
    }

    #[test]
    fn number_eq_is_lossless_decimal() {
        let cases = [
            ("1", "1.0"),
            ("1", "1e0"),
            ("1", "100e-2"),
            ("1.0", "100e-2"),
            ("-0", "0"),
            ("-0.0", "0e5"),
            ("0.01", "1e-2"),
            ("12300", "12.3e3"),
            ("-1.230", "-123e-2"),
            ("0.5", "5E-1"),
            ("1000000000000000000000", "1e21"),
            ("9007199254740993", "9007199254740993.0"),
            ("0.000000000000000000001", "1e-21"),
            ("1e999999999999999999999", "10e999999999999999999998"),
            ("1e-999999999999999999999", "0.1e-999999999999999999998"),
        ];
        for (a, b) in cases {
            assert!(number_eq(&number(a), &number(b)), "expected {a} == {b}");
        }

        let different = [
            ("9007199254740993", "9007199254740992.0"),
            ("9007199254740993", "9007199254740992"),
            ("1", "1.0000000000000001"),
            ("0.1", "0.10000000000000001"),
            ("1e999999999999999999999", "2e999999999999999999999"),
            ("1e-999999999999999999999", "2e-999999999999999999999"),
            ("1e21", "1e22"),
            ("-1", "1"),
        ];
        for (a, b) in different {
            assert!(!number_eq(&number(a), &number(b)), "expected {a} != {b}");
        }
    }

    fn number(lexeme: &str) -> Number {
        let value: Value = serde_json::from_str(lexeme).unwrap();
        value.as_number().unwrap().clone()
    }

    #[test]
    fn diff_detects_precision_differences_everywhere() {
        // 同值不同写法（嵌套对象与数组中）不产生 change。
        let result = diff_ok(
            serde_json::from_str(
                r#"{
                "before": [{"id": "r", "values": {"a": [1, {"b": 100e-2}], "c": -0}}],
                "after":  [{"id": "r", "values": {"a": [1.0, {"b": 1e0}], "c": 0}}]
            }"#,
            )
            .unwrap(),
        );
        assert_eq!(result, json!({"changes": []}));

        // 2^53 附近的真实差异必须被发现，且原始写法原样回传。
        let result = diff_ok(serde_json::from_str(
            r#"{
                "before": [{"id": "r", "values": {"x": 9007199254740992.0, "deep": {"y": [1e21]}}}],
                "after":  [{"id": "r", "values": {"x": 9007199254740993, "deep": {"y": [1000000000000000000001]}}}]
            }"#,
        ).unwrap());
        assert_eq!(
            result,
            serde_json::from_str::<Value>(
                r#"{"changes":[{"id":"r","op":"update","fields":[
                    {"path":"/deep/y","old":[1e21],"new":[1000000000000000000001]},
                    {"path":"/x","old":9007199254740992.0,"new":9007199254740993}
                ]}]}"#
            )
            .unwrap()
        );
    }

    #[test]
    fn apply_accepts_equivalent_forms_and_rejects_real_precision_diff() {
        // old 用不同写法表示同一数值 → 通过，new 精确写入。
        let result = apply_ok(
            serde_json::from_str(
                r#"{
                "rows": [{"id": "r", "values": {"x": 9007199254740993, "y": [1e21]}}],
                "changes": [{"id": "r", "op": "update", "fields": [
                    {"path": "/x", "old": 9007199254740993.000, "new": 100e-2},
                    {"path": "/y", "old": [1000000000000000000000], "new": 1e-999}
                ]}],
                "direction": "forward"
            }"#,
            )
            .unwrap(),
        );
        assert_eq!(
            result,
            serde_json::from_str::<Value>(
                r#"{"rows":[{"id":"r","values":{"x":100e-2,"y":1e-999}}]}"#
            )
            .unwrap()
        );

        // 2^53 边界相差 1 → 409，path 保持原样，且不部分应用。
        let err = apply(
            &serde_json::from_str(
                r#"{
                    "rows": [{"id": "r", "values": {"x": 9007199254740993}}],
                    "changes": [{"id": "r", "op": "update", "fields": [
                        {"path": "/x", "old": 9007199254740992.0, "new": 1}
                    ]}],
                    "direction": "forward"
                }"#,
            )
            .unwrap(),
        )
        .unwrap_err();
        assert_eq!(err.status, 409);
        assert_eq!(err.path, "/changes/0/fields/0/old");

        // add 行整体值：等价值写法通过反向校验。
        apply_ok(
            serde_json::from_str(
                r#"{
                "rows": [{"id": "r", "values": {"n": 1}}],
                "changes": [{"id": "r", "op": "add", "value": {"n": 1.0}}],
                "direction": "reverse"
            }"#,
            )
            .unwrap(),
        );
    }
}
