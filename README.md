# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供数据集的批量写入、版本快照保存与历史快照读取能力。所有改变持久状态的操作都会追加写入只追加日志（journal），服务重启后按日志重放恢复当前数据集与全部快照，具备崩溃后恢复能力。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 持久化与恢复

- 持久化目录由环境变量 `VDE_DATA_DIR` 指定，默认 `./data`；启动时若目录不存在会自动创建。
- 目录下的只追加日志文件固定命名为 `journal.log`。日志只追加，从不改写已有行。
- 每次成功改变持久状态的操作都向日志追加一行 JSON 对象：
  - 批次写入提交时，批次内每条记录追加一行：
    ```json
    {"type":"write","key":"alpha","fields":{"n":1}}
    ```
    `type` 字面值为 `"write"`，另有 `key`（字符串键）与 `fields`（该记录的字段对象）。对已存在键的覆盖在重放时按相同顺序再次整体覆盖，语义与在线一致。
  - 保存快照时追加一行：
    ```json
    {"type":"snapshot","version":1,"total":2}
    ```
    `type` 字面值为 `"snapshot"`，另有 `version`（该快照版本号）与 `total`（保存时刻数据集记录总数）。快照内容本身不入日志——重放到该行时对当前数据集做完整深拷贝即得到该快照。
- 每行写入后立即落盘（`fsync`）。进程崩溃后重启，服务按日志行顺序重放重建当前数据集与全部快照；空日志文件或目录不存在等价于空数据集、零快照。
- 恢复后 `POST /versions` 的下一版本号在已恢复的最大版本号上递增 1，不会从 1 重来。
- 若某行结构非法（不是 JSON 对象、`type` 不是 `"write"`/`"snapshot"` 两个字面值之一，或缺少该行必需字段：write 缺 `key`/`fields`、snapshot 缺 `version`/`total`），则**自该行起的全部内容被丢弃**（文件截断到该行行首），此前已重放的状态保留，服务照常启动，并在 stdout 输出一行说明被丢弃的起始行号（从 1 计），例如：
  ```
  日志 ./data/journal.log 第 8 行结构非法，已丢弃自该行起的全部内容
  ```
- 若合法日志末行缺少结尾换行（例如上次进程崩溃恰好留下一行完整记录），启动时会补一个换行再重放，避免后续追加与之粘连。
- 日志写入失败（磁盘满、目录不可写等）时：
  - 该操作返回 `500`，响应体 `error` 说明原因；
  - 不改变内存中的数据集或快照（快照版本号也不消耗）；
  - 日志中不会留下半条记录（文件截回该操作之前的长度）；
  - `/datasets/records` 仍整批要么全成功要么全失败。

## 接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/health` | GET | 健康检查 |
| `/version` | GET | 服务名与版本号 |
| `/datasets/records` | POST | 批量写入记录 |
| `/versions` | POST | 保存当前数据集为不可变快照 |
| `/snapshots?version=N[&key=K]` | GET | 读取历史快照（全部记录或单个键） |

### GET /health

```json
{"status":"ok"}
```

### GET /version

```json
{"name":"versioned-data-engine","version":"0.1.0"}
```

### POST /datasets/records

请求体为一个 JSON 对象，`records` 是一批记录；每条记录包含唯一的字符串键 `key` 和 JSON 对象字段集 `fields`。

- 批次要么整体成功、要么整体失败：任一条记录非法时，整个批次不生效，数据集保持请求前的状态。
- 同一批次内不允许重复键；对已存在的键再次写入表示整体覆盖该记录。
- 空批次非法。
- 成功返回 `200`、本批写入条数 `written` 与数据集当前记录总数 `total`。
- 失败返回 `400`（结构非法、缺键、键不是字符串、字段集不是对象、空批次）或 `409`（批次内重复键），响应中 `error` 说明原因，`index` 为出错记录在批次中的下标（从 0 开始）。
- 日志落盘失败返回 `500`，响应中 `error` 说明原因；此时整批不生效，日志也不留半条记录。

```sh
curl -s -X POST http://127.0.0.1:8080/datasets/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"key":"alpha","fields":{"n":1}},{"key":"bravo","fields":{"n":2}}]}'
# 200 {"written":2,"total":2}

curl -s -X POST http://127.0.0.1:8080/datasets/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"key":"alpha","fields":{"n":1}},{"key":"alpha","fields":{"n":2}}]}'
# 409 {"error":"批次内出现重复键: alpha","index":1}
```

### POST /versions

把数据集当前内容固化为不可变快照。首次启动版本号从 1 开始，每次成功保存严格递增 1；之后继续写入数据集不会改变任何已保存快照。进程重启后从日志恢复，版本号在已恢复的最大版本号上继续递增。保存空数据集也合法，得到记录数为 0 的空快照。返回新版本号 `version` 与该快照的记录总数 `total`。日志落盘失败时返回 `500`，不生成快照、不消耗版本号。

```sh
curl -s -X POST http://127.0.0.1:8080/versions
# {"version":1,"total":2}
```

### GET /snapshots

按版本号读取已保存快照：

- 只给 `version`：列出快照中全部记录，按键的字典序排列。
- 同时给出 `key`：只返回该键在这份快照中的记录。
- 版本不存在或尚未保存、或快照中不存在该键时返回 `404`，且不会产生新快照。
- 缺少 `version` 或其不是正整数时返回 `400`。

```sh
curl -s 'http://127.0.0.1:8080/snapshots?version=1'
# {"version":1,"records":[{"key":"alpha","fields":{"n":1}},{"key":"bravo","fields":{"n":2}}]}

curl -s 'http://127.0.0.1:8080/snapshots?version=1&key=alpha'
# {"version":1,"key":"alpha","fields":{"n":1}}

curl -s 'http://127.0.0.1:8080/snapshots?version=9'
# 404 {"error":"版本 9 不存在或尚未保存"}
```

## 并发语义

写入、保存与读取在服务内部通过同一把锁串行化临界区：批次写入先整批追加日志并落盘、再一次性提交内存；快照保存也是日志落盘成功后才在保存时刻对数据集做完整深拷贝。因此任何一份已保存快照都与某次保存时刻的完整数据集一致，不会出现半批数据；落盘失败时内存与日志一起保持操作前状态。
