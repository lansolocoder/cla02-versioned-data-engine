# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。当前仅提供健康状态和版本信息，尚无数据写入、存储或查询功能。

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
| `GET /datasets/{name}/records` | 200 `{"name":<数据集名>,"records":[{"key":<key>,"value":<JSON 值>,"updatedAt":<RFC3339 UTC>},...]}`，records 按 key 的 Unicode 码点升序；支持按键前缀或键区间筛选（见下文「记录筛选」）；400 `{"error":"invalid_identifier"}` / `{"error":"invalid_query"}`，404 `{"error":"not_found"}`，500 `{"error":"internal"}` |

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
