# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及内存数据集的批量写入、版本快照保存与历史快照读取。数据仅保存在内存中，进程重启后全部丢失，不做持久化。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 基础接口

| 接口 | 响应 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |

## 数据模型

数据集是内存中的记录集合，每条记录由唯一的字符串键 `key` 和一个 JSON 对象 `fields` 构成。
快照是某次保存时刻数据集的完整不可变副本；版本号从 1 开始，每次成功保存严格递增 1。

## `POST /records` — 批量写入

请求体为记录数组，按批次整体生效：同一批次内不允许重复键；对已存在的键再次写入表示整体覆盖该记录。
任何一条记录不合法都会导致整个批次失败，数据集保持请求前的状态。空批次也是非法的。

```sh
curl -s -X POST http://127.0.0.1:8080/records \
  -H 'Content-Type: application/json' \
  -d '[{"key":"alice","fields":{"role":"admin"}},{"key":"bob","fields":{"role":"user"}}]'
```

成功响应 `200 OK`：

```json
{"written":2,"total":2}
```

失败响应 `400 Bad Request`，`index` 为出错记录在批次中的下标（从 0 开始）：

```json
{"error":"duplicate key within the same batch","index":1}
```

以下情况均会使整个批次失败（请求体不是 JSON 数组、记录不是对象、缺少 `key`、`key` 不是字符串、缺少 `fields`、`fields` 不是 JSON 对象、批次内键重复、空批次），与请求体不是合法 JSON 一样返回 `400` 与说明原因的 JSON。

## `POST /versions` — 保存版本快照

把数据集当前内容固化为不可变快照。返回新版本号与快照记录总数；快照生成后再写入数据集不影响任何已保存快照。空数据集也可以保存，得到记录数为 0 的空快照。

```sh
curl -s -X POST http://127.0.0.1:8080/versions
```

```json
{"version":1,"total":2}
```

## `GET /versions/{version}` — 读取历史快照

只给出版本号时列出该快照的全部记录，按键的字典序排列：

```sh
curl -s http://127.0.0.1:8080/versions/1
```

```json
{"version":1,"records":[{"key":"alice","fields":{"role":"admin"}},{"key":"bob","fields":{"role":"user"}}]}
```

通过可选查询参数 `key` 只读取一条记录（参数值按 URL 规则百分号编码）：

```sh
curl -s 'http://127.0.0.1:8080/versions/1?key=alice'
```

```json
{"version":1,"records":[{"key":"alice","fields":{"role":"admin"}}]}
```

版本号不存在（含尚未保存、0 或非数字）或该版本中不存在给定键时，返回 `400 Bad Request`：

```json
{"error":"version 9 does not exist"}
```

```json
{"error":"key `alice` not found in version 2"}
```

读取操作不会创建新快照。并发的写入、保存与读取相互交错时，每份快照都与某次保存时刻的完整数据集一致，不会出现半批数据。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
```
