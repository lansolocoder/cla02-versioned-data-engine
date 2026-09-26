# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供数据集的批量写入、版本快照保存与历史快照读取能力。当前数据集与全部快照会持久化到本地数据文件，服务重启后自动恢复，版本号序列延续。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

`VDE_DATA_DIR` 指定持久化数据文件所在目录，默认是当前工作目录下的 `data`；状态文件名固定为 `state.json`（即默认路径 `./data/state.json`）。目录不存在时服务会自动创建。详见下文[持久化与恢复](#持久化与恢复)。

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

把数据集当前内容固化为不可变快照。版本号从 1 开始，每次成功保存严格递增 1；之后继续写入数据集不会改变任何已保存快照。保存空数据集也合法，得到记录数为 0 的空快照。返回新版本号 `version` 与该快照的记录总数 `total`。

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

## 持久化与恢复

当前数据集与全部快照保存在数据文件 `<VDE_DATA_DIR>/state.json` 中：

- 目录由环境变量 `VDE_DATA_DIR` 指定；未设置时使用当前工作目录下的 `data`，服务会自动创建该目录。
- 每次成功的批量写入（`POST /datasets/records`）与快照保存（`POST /versions`）都会把最新整体状态原子写入数据文件（先写临时文件并落盘，再重命名覆盖），因此进程被强制终止或崩溃时文件始终是某一份完整状态。
- 启动时若数据文件不存在，服务按空数据集、尚未保存任何快照的状态启动。
- 启动时若数据文件存在，则从中恢复当前数据集与全部历史快照：已保存快照可按原版本号读取，内容（字段名、字段值、数值类型与嵌套结构）逐字节一致；恢复后再调用 `POST /versions`，新版本号严格接在旧序列之后（旧最大版本为 N，恢复后第一个成功保存得到 N+1）。
- 若数据文件存在但无法解析（内容被截断、不是合法 JSON）或结构不符合要求（缺少/错配字段、快照版本号不从 1 起严格连续），服务拒绝启动：以非 0 退出码退出，错误信息写入标准错误，不会改动原数据文件，也不会把半份数据载入内存后对外服务。

## 并发语义

写入、保存与读取在服务内部通过同一把锁串行化临界区：批次写入一次性提交，快照在保存时刻对数据集做完整深拷贝。因此任何一份已保存快照都与某次保存时刻的完整数据集一致，不会出现半批数据。
