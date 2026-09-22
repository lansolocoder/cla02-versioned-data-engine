# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序，提供无状态的快照差异计算（/diff）与双向回放（/apply）。差异与回放逻辑在核心模块 `src/engine.rs` 中实现，HTTP 层仅负责 JSON 映射。

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
| `POST /diff` | `{"changes":[...]}` |
| `POST /apply` | `{"rows":[...]}` |

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
```

## POST /diff

接收 `{"before": [...], "after": [...]}`，均为 `{"id", "values"}` 行数组；`id` 为非空唯一字符串，`values` 为对象。返回 `{"changes": [...]}`：

- 新增 / 删除：`{"id", "op": "add"|"remove", "value"}`；
- 内容变化：`{"id", "op": "update", "fields": [...]}`，`fields` 递归比较对象，元素为 `{"path", "old"?, "new"?}`；
- `path` 为 RFC 6901 JSON Pointer，字段在某侧缺失时省略该侧（与 JSON `null` 区分）；数组与非对象值整体比较；
- 对象键序不影响相等，数字按数学值作无损十进制比较（不经过 f64；`1`、`1.0`、`1e0`、`100e-2` 相等，而 `9007199254740993` 与 `9007199254740992.0` 不同），响应中的数字逐字保留输入写法；
- `changes` 按 `id`、`fields` 按 `path` 升序。

```sh
curl -X POST http://127.0.0.1:8080/diff -d '{
  "before": [{"id": "u1", "values": {"age": 30}}],
  "after":  [{"id": "u1", "values": {"age": 31}}]
}'
# {"changes":[{"fields":[{"new":31,"old":30,"path":"/age"}],"id":"u1","op":"update"}]}
```

## POST /apply

接收 `{"rows": [...], "changes": [...], "direction": "forward"|"reverse"}`，返回按 `id` 升序的 `{"rows": [...]}`。正向应用 `old`→`new`，反向还原。应用前校验结构、路径唯一性、行存在性与预期旧值（区分缺失与 `null`）；任一校验失败返回 409 且不部分应用，格式错误返回 400。

错误体统一为 `{"error": {"code", "message", "path"}}`，`path` 定位请求体中的出错字段。

```sh
curl -X POST http://127.0.0.1:8080/apply -d '{
  "rows": [{"id": "u1", "values": {"age": 30}}],
  "changes": [{"id": "u1", "op": "update", "fields": [{"path": "/age", "old": 30, "new": 31}]}],
  "direction": "forward"
}'
# {"rows":[{"id":"u1","values":{"age":31}}]}
```
