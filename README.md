# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及纯内存的**版本化数据集**：每次提交生成一个不可变快照，支持按版本读取、`request_id` 幂等与乐观并发控制。数据仅存于内存，进程重启即丢失。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

| 接口 | 说明 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |
| `POST /datasets/{name}/commits` | 提交一次变更，创建新版本 |
| `GET /datasets/{name}/snapshots/{version}` | 读取快照，`version` 可为正整数或 `latest` |

## 提交变更

请求体：

- `base`：所基于的当前版本；数据集首次提交为 `null`，之后为最新版本号。
- `request_id`：非空字符串，在同一数据集内用于幂等去重。
- `upserts`：`[{"key": ..., "value": ...}]`，key 非空，value 为任意 JSON。
- `deletes`：要删除的 key 字符串数组。
- `upserts` 与 `deletes` 不能同时为空；同一请求内 key 不得重复，也不得同时出现在两处。

成功返回 `201`：

```sh
curl -X POST http://127.0.0.1:8080/datasets/sales/commits \
  -H 'content-type: application/json' \
  -d '{"base":null,"request_id":"req-1","upserts":[{"key":"apple","value":1},{"key":"banana","value":{"price":3}}],"deletes":[]}'
# 201 {"dataset":"sales","version":1,"request_id":"req-1"}

curl -X POST http://127.0.0.1:8080/datasets/sales/commits \
  -H 'content-type: application/json' \
  -d '{"base":1,"request_id":"req-2","upserts":[{"key":"apple","value":9}],"deletes":["banana"]}'
# 201 {"dataset":"sales","version":2,"request_id":"req-2"}
```

各数据集版本从 1 起逐 1 递增。

- **乐观并发**：`base` 不是当前版本（含对已存在数据集使用 `null`）返回 `409 stale_base`，且不产生任何改动。相同 base 的并发提交只有一个成功。
- **幂等**：同一数据集、同一 `request_id` 且内容相同的重复提交，重放首次响应、不新增版本——即使首次提交所用的 `base` 现已过期；内容不同则返回 `409 idempotency_conflict`。
- **校验失败**（空变更、空名称/key、重复 key、key 同时写删、字段类型错误、畸形 JSON）返回 `400`，不产生版本或改动。

## 读取快照

```sh
curl http://127.0.0.1:8080/datasets/sales/snapshots/1
# {"dataset":"sales","version":1,"records":[{"key":"apple","value":1},{"key":"banana","value":{"price":3}}]}

curl http://127.0.0.1:8080/datasets/sales/snapshots/latest
# {"dataset":"sales","version":2,"records":[{"key":"apple","value":9}]}
```

`records` 按 key 的 Unicode 码点顺序排列；每条记录含 `key` 与 `value`。历史快照不可变，不会随后续提交变化。未知数据集返回 `404 dataset_not_found`，未知版本返回 `404 snapshot_not_found`。读取只可能看到某次提交完成前或完成后的完整状态。

## 错误格式

所有错误均为 JSON，含稳定的 `code` 与可读的 `message`：

```json
{"code":"stale_base","message":"base 1 已过期，当前版本为 2"}
```

| HTTP | code | 触发场景 |
| --- | --- | --- |
| 400 | `invalid_json` | 请求体不是合法 JSON |
| 400 | `invalid_request` | 字段缺失/类型错误，或业务校验失败（空变更、空 key 等） |
| 404 | `dataset_not_found` / `snapshot_not_found` / `not_found` | 数据集、版本或路由不存在 |
| 405 | `method_not_allowed` | 路径存在但请求方法不支持 |
| 409 | `stale_base` | base 已过期 |
| 409 | `idempotency_conflict` | 同一 request_id 提交了不同内容 |
