# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供数据集的批量写入、版本快照保存与历史快照读取能力。默认数据仅保存在内存中，进程重启后丢失；设置 `VDE_DATA_DIR` 后可把每次写入批次与版本快照崩溃安全地落到磁盘，重启后完整恢复（见下文「持久化」）。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
# 纯内存模式（默认，重启丢失）
VDE_BIND=127.0.0.1:8080 cargo run --locked
# 持久化模式（重启后恢复全部历史）
VDE_DATA_DIR=/var/lib/vde VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。`VDE_DATA_DIR` 指定持久化目录（见下文）。按 Ctrl-C 停止服务。

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

## 持久化（VDE_DATA_DIR）

- 不设置 `VDE_DATA_DIR`：服务照旧纯内存运行，不创建、不写入任何文件，重启后数据丢失。
- 设置 `VDE_DATA_DIR`：目录必须已存在或服务可创建（含其父目录可写）。每次成功的批次写入都会把该批次落盘；每次成功保存版本都会把该版本的完整快照落盘。HTTP/JSON 接口的行为、状态码与错误语义（400/409/404、`error` 与 `index` 字段、空批次非法、键的字典序、快照不可变等）与纯内存模式完全一致。

### 磁盘布局

```text
$VDE_DATA_DIR/
├── lock                                  # 目录占用锁（flock，进程退出即释放）
├── writes/00000000000000000001.json      # 每个成功写入批次一个不可变事件文件
└── snapshots/00000000000000000001.json   # 每个成功保存版本一个完整快照文件
```

每个文件由一行头部（格式版本、payload 字节数、CRC32 校验和）与 JSON payload 组成。写入事件文件与快照文件一经 rename 提交即不可变，不要手工编辑或删除。

### 恢复保证

进程重启时读取该目录并恢复出与上次退出前**完全一致**的状态：

- 数据集当前内容（最新快照之后的写入会重放）；
- 全部历史快照，包括记录数为 0 的空快照；
- 版本号继续从重启前最后一个版本之后严格递增，不重复、不跳号；恢复后 `GET /snapshots?version=N` 与 `&key=K` 的结果与重启前逐字节一致。

### 崩溃安全

保存版本与批次写入的落盘都是原子的：先写同目录临时文件并 `fsync`，再 `rename` 为最终文件名，最后 `fsync` 目录。rename 即提交点。因此在写入过程中被强杀，重启后要么能看到这次保存的**完整**快照（版本号、记录数、每条记录内容均正确），要么完全看不到它且版本号不前进；不会出现半截快照、版本号跳号或数据集与快照不一致。崩溃残留的临时文件在下次启动时自动清理（它们从未提交）。

### 拒绝启动的条件

以下情况服务会**拒绝启动**：向 stderr 打印错误、以非零退出码退出，且不会部分加载数据、不会覆盖或修改目录中的任何文件：

1. **目录被占用**：另一个存活的服务实例持有该目录的锁。多个进程不能共用同一持久化目录。
2. **数据损坏或格式无法解析**：包括文件无法读取/解析、CRC32 校验失败、写入事件或快照编号不连续（有文件缺失）、快照内容与事件重放结果不一致、快照内出现重复键、数据目录中出现无法识别的文件等。

若因损坏拒绝启动，请在排查后从备份恢复或清空目录（其中数据将全部丢失）再启动。
