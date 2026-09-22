# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序，提供无状态快照差异计算与双向回放。所有 diff/apply 逻辑在核心模块 `src/engine.rs` 中实现，HTTP 层仅负责 JSON 映射。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

| 接口 | 响应 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |
| `POST /diff` | 计算两个快照之间的变更 |
| `POST /apply` | 正向/反向回放变更 |

## `POST /diff`

请求体 `{"before": [...], "after": [...]}`，均为 `{"id", "values"}` 行数组；`id` 是非空唯一字符串，`values` 是对象。返回 `{"changes": [...]}`：

- 新增/删除行：`{"id", "op": "add"|"remove", "value"}`
- 内容变化的行：`{"id", "op": "update", "fields": [{"path", "old"?, "new"?}, ...]}`

`fields` 递归比较对象；`path` 为 RFC 6901 JSON Pointer；字段在某侧缺失时省略该侧；数组与非对象值整体比较。对象键序不影响相等，数字按数学值比较。`changes` 按 `id`、`fields` 按 `path` 升序。

## `POST /apply`

请求体 `{"rows": [...], "changes": [...], "direction": "forward"|"reverse"}`，返回按 `id` 升序的 `{"rows": [...]}`。`forward` 应用 old→new，`reverse` 还原。应用前校验结构、路径唯一性、行存在性与预期旧值（区分缺失与 null）；冲突返回 409 且不部分应用，格式错误返回 400。

错误响应统一为 `{"error": {"code", "message", "path"}}`，`path` 定位出错的请求字段。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
curl -X POST http://127.0.0.1:8080/diff \
  -d '{"before":[{"id":"a","values":{"x":1}}],"after":[{"id":"a","values":{"x":2}}]}'
curl -X POST http://127.0.0.1:8080/apply \
  -d '{"rows":[{"id":"a","values":{"x":1}}],"direction":"forward",
       "changes":[{"id":"a","op":"update","fields":[{"path":"/x","old":1,"new":2}]}]}'
```
