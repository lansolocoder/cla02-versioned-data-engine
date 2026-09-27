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
| `GET /datasets/{name}/records` | 200 `{"name":<数据集名>,"records":[{"key":<key>,"value":<JSON 值>,"updatedAt":<RFC3339 UTC>},...]}`，records 按 key 的 Unicode 码点升序；支持按键筛选的查询参数（见下）；400 `{"error":"invalid_identifier"}` / `{"error":"invalid_query"}`，404 `{"error":"not_found"}`，500 `{"error":"internal"}` |

### 记录列表的键筛选

`GET /datasets/{name}/records` 接受以下可选查询参数，均为按键（key）筛选，可组合使用，多个条件取交集（AND）：

| 参数 | 含义 |
| --- | --- |
| `prefix=<串>` | key 以该字符串开头（字符串前缀匹配） |
| `gte=<key>` | key ≥ 下界（闭区间，含下界） |
| `gt=<key>` | key > 下界（开区间，不含下界） |
| `lte=<key>` | key ≤ 上界（闭区间，含上界） |
| `le=<key>` | key < 上界（开区间，不含上界） |

比较按 key 的 Unicode 码点顺序（与结果排序一致）。规则：

- 参数值必须非空且只含 `[A-Za-z0-9_-]`（与 key 的字符集一致）。
- 每个参数至多出现一次；`gte` 与 `gt` 互斥，`lte` 与 `le` 互斥。
- 除上述五个参数外不允许任何其他查询参数。
- 违反以上任一规则（未知参数、重复参数、冲突区间界、空值或含非法字符的值）一律返回 400 `{"error":"invalid_query"}`。
- 下界大于上界不属于错误，只是命中空集：筛选无命中时返回 200 与空 `records` 数组，不返回 404。
- 不带任何查询参数时行为与全量列表完全一致。

示例：`GET /datasets/users/records?prefix=user-0&gte=user-010&le=user-099` 返回 key 以 `user-0` 开头、且 `user-010` ≤ key < `user-099` 的全部记录。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
```
