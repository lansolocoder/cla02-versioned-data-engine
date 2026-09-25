# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供内存数据集的批量写入、版本快照保存与历史快照读取能力。默认纯内存运行、重启即丢；设置 `VDE_DATA_DIR` 后开启磁盘持久化，重启可完整恢复。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 持久化

设置环境变量 `VDE_DATA_DIR` 后，服务把数据落到该目录；未设置时保持纯内存运行，重启丢失数据，不写任何文件。

```sh
VDE_DATA_DIR=/var/lib/vde VDE_BIND=127.0.0.1:8080 cargo run --locked
```

目录不存在时由服务自动创建。目录内容：

| 文件 | 内容 |
| --- | --- |
| `dataset.json` | 当前数据集，每次成功写入批次后整体原子替换 |
| `snapshot-N.json` | 第 N 版快照（`{"version":N,"records":{...}}`），保存版本时原子写入，之后不可变 |
| `vde.lock` | 进程级排他锁文件 |

### 恢复保证

- 每次成功的批次写入先把新数据集落盘再提交内存；每次保存版本先把快照文件落盘再推进版本号。进程重启（包括 `kill -9`）后，恢复出的数据集当前内容与全部历史快照与重启前完全一致，版本号从重启前最后一个版本之后严格递增，不重复、不跳号。
- 所有落盘都走「写临时文件 + fsync + rename + 目录 fsync」：写盘过程中进程被强杀，重启后要么看到这次保存的完整快照，要么完全看不到且版本号不前进，不会出现半截快照或数据集与快照不一致。残留的临时文件（`*.tmp`）在启动时被忽略。
- 空数据集保存的空快照（记录数 0）同样被持久化并恢复。

### 拒绝启动的条件

以下情况服务拒绝启动，退出码非 0，错误信息写到 stderr，且不改动目录中已有的任何数据：

- 目录已被其他运行中的实例占用（同一持久化目录不允许多个进程共用；实例退出后锁自动释放）；
- 目录中数据损坏或格式无法解析（如 `dataset.json` 不是合法 JSON、快照文件解析失败、文件内版本号与文件名不符、快照版本不连续），此时不会部分加载，也不会覆盖损坏文件；
- 目录不可创建或不可读写。

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

## 并发语义

写入、保存与读取在服务内部通过同一把锁串行化临界区：批次写入一次性提交，快照在保存时刻对数据集做完整深拷贝。因此任何一份已保存快照都与某次保存时刻的完整数据集一致，不会出现半批数据。
