# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及按数据集组织的记录单条/批量写入与查询能力。

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
| `PUT /datasets/{name}/records/{key}` | 200 `{"key":<key>,"value":<JSON 值>}`，body 为该记录的任意 JSON 值；key 已存在则覆盖。400 `{"error":"invalid_identifier"}` / `{"error":"invalid_json"}`，500 `{"error":"internal"}` |
| `GET /datasets/{name}/records/{key}` | 200 `{"key":<key>,"value":<JSON 值>}`；400 `{"error":"invalid_identifier"}`，404 `{"error":"not_found"}`，500 `{"error":"internal"}` |
| `PUT /datasets/{name}/records` | 批量写入（见下文「批量写入」）：200 `{"name":<数据集名>,"records":[{"key":<key>,"value":<JSON 值>,"updatedAt":<RFC3339 UTC>},...]}`，records 按 key 的 Unicode 码点升序；400 `{"error":"invalid_identifier"}` / `{"error":"invalid_json"}`，500 `{"error":"internal"}` |
| `GET /datasets/{name}/records` | 200 `{"name":<数据集名>,"records":[{"key":<key>,"value":<JSON 值>,"updatedAt":<RFC3339 UTC>},...]}`，records 按 key 的 Unicode 码点升序；支持按键前缀或键区间筛选（见下文「记录筛选」）；400 `{"error":"invalid_identifier"}` / `{"error":"invalid_query"}`，404 `{"error":"not_found"}`，500 `{"error":"internal"}` |

### 批量写入

`PUT /datasets/{name}/records` 在一个事务里写入整批记录，整批要么全部生效，要么一条都不写。请求体为：

```json
{"records":[{"key":<key>,"value":<JSON 值>},...]}
```

规则：

- 数据集名与每个 key 沿用既有标识符规则（非空，仅含 `[A-Za-z0-9_-]`）；任一不合法返回 400 `{"error":"invalid_identifier"}`。
- body 为空、不是合法 JSON、`records` 缺失或不是数组、条目缺 `key`/`value`、同一批次出现重复 key，一律返回 400 `{"error":"invalid_json"}`，数据集保持原样。
- key 已存在则覆盖旧值并更新 `updatedAt`，不存在则新增。
- 空 `records` 数组是合法批次：返回 200 与空 `records` 数组，不改动数据集。
- 写入过程中任一步骤失败时回滚本次已改动记录到请求前的值与 `updatedAt`，返回 500 `{"error":"internal"}`；失败后读到的数据集与请求前完全一致。
- 成功返回 200，响应体中的 `updatedAt` 为该条写入完成时刻（RFC3339 UTC，格式与记录列表一致），records 按 key 的 Unicode 码点升序。
- 写入对并发请求是原子可见的：其他请求只能看到写入前或写入后的完整数据集，不会看到写到一半的批次；进程崩溃或被杀后重启，数据集恢复为整个批次生效或完全未生效的某个完整状态。
- 写入成功后，单条 GET 与记录列表（含前缀与区间筛选）立即反映新值，排序与 `updatedAt` 语义不变。

示例：

```sh
curl -X PUT http://127.0.0.1:8080/datasets/ds1/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"key":"user-1","value":{"name":"ada"}},{"key":"user-2","value":42}]}'
```

### 记录筛选

`GET /datasets/{name}/records` 可通过查询参数按 key 筛选，筛选只作用于 key，响应格式与全量列表完全一致；不带任何查询参数时行为与全量列表相同。支持的参数（均为可选）：

| 参数 | 含义 |
| --- | --- |
| `prefix=<值>` | 前缀匹配：保留 key 以 `<值>` 开头的记录 |
| `gte=<值>` | 区间下界（闭）：保留 key ≥ `<值>` 的记录 |
| `gt=<值>` | 区间下界（开）：保留 key > `<值>` 的记录 |
| `lte=<值>` | 区间上界（闭）：保留 key ≤ `<值>` 的记录 |
| `lt=<值>` | 区间上界（开）：保留 key < `<值>` 的记录 |

比较均为 key 字符串的 Unicode 码点序。规则：

- 参数取值必须非空且只含 `[A-Za-z0-9_-]`（与 key 的字符集一致），需按 URL 查询参数规则做百分号编码。
- `prefix` 与区间参数（`gte`/`gt`/`lte`/`lt`）互斥，不得同时出现。
- 下界至多一个（`gte` 与 `gt` 二选一），上界至多一个（`lte` 与 `lt` 二选一）；下界与上界可同时使用，构成区间。
- 除上述参数外的任何查询参数、重复参数、空取值或无法解析的取值，一律返回 400 `{"error":"invalid_query"}`。
- 筛选无命中时返回 200 与空 `records` 数组，不返回 404。

示例：`GET /datasets/ds1/records?prefix=user-0`、`GET /datasets/ds1/records?gte=user-000&lt=user-100`。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
```
