# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供数据集的批量写入、版本快照保存与历史快照读取能力。每次 `POST /versions` 成功提交后，版本记录、快照内容与当时的数据集内容会一起原子落盘到 `VDE_DATA_DIR`，进程重启后自动恢复。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_DATA_DIR=./data VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 持久化与恢复

- `VDE_DATA_DIR` 指定持久化根目录。**未设置或值为空属于配置错误**：服务打印一行说明后以非零码退出，且不会创建任何目录。
- 启动时目录不存在会自动创建；目录已存在则加载其中的持久化状态：版本号从已保存版本继续分配（`POST /versions` 为最大已恢复版本号加 1），历史快照与 diff 结果与重启前一致。目录中没有任何持久化文件时按全新实例启动，版本号从 1 开始。
- `VDE_DATA_DIR` 指向普通文件而非目录时同样是配置错误：打印一行说明后非零退出，不会覆盖或删除该文件。
- 每个版本在根目录下对应一个独立条目文件（20 位零填充版本号 + `.vde`，字典序即提交顺序），内含 JSON 载荷、首尾魔数与 CRC32 校验。
- **原子提交**：写入同目录临时文件并 `fsync`，再原子 `rename` 为最终文件名。提交前崩溃或写盘失败不会留下本次保存的任何痕迹；写盘失败时整次保存失败，返回 `500` 与含 `error` 字段的 JSON，数据集保持保存前内容，版本号不递增，下次保存复用同一版本号。
- **崩溃恢复**：条目按持久化顺序逐个独立校验，半写、截断或校验不通过的条目按不存在跳过，其后完整且校验通过的条目照常恢复；无法确定完整性时以最后一个完整条目为准，其后数据一律丢弃。崩溃遗留的临时文件启动时自动清除。

## 接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/health` | GET | 健康检查 |
| `/version` | GET | 服务名与版本号 |
| `/datasets/records` | POST | 批量写入记录 |
| `/versions` | POST | 保存当前数据集为不可变快照 |
| `/snapshots?version=N[&key=K]` | GET | 读取历史快照（全部记录或单个键） |
| `/snapshots/diff?from=N&to=M` | GET | 比较两份快照之间的记录级变化 |

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

把数据集当前内容固化为不可变快照并原子落盘。版本号从 1 开始，每次成功保存严格递增 1；之后继续写入数据集不会改变任何已保存快照。保存空数据集也合法，得到记录数为 0 的空快照。返回新版本号 `version` 与该快照的记录总数 `total`。写盘失败时返回 `500`（响应体为含 `error` 字段的 JSON 对象），数据集与版本号都保持保存前的状态，下次保存复用同一版本号。

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

### GET /snapshots/diff

比较两份已保存快照之间的记录级变化。`from` 与 `to` 均为必填的正整数版本号，可以相等（此时全部记录为 `unchanged`）。

- 返回 `from`、`to` 与 `changes` 数组；`changes` 覆盖两份快照键的并集，按键字典序升序排列。
- 每项含 `key`、`change`（`added` / `removed` / `modified` / `unchanged`）与 `fields`：`added` 时为 `to` 侧字段集，`removed` 时为 `from` 侧字段集，`modified` 时为 `{"from":…,"to":…}`，`unchanged` 时为 `null`。
- 字段集是否相同按 JSON 值语义判断，与对象内字段顺序无关。
- `from` 或 `to` 指向尚未保存的版本时返回 `404`；缺少任一参数或参数不是正整数时返回 `400`。失败响应均为含 `error` 字段的 JSON 对象，且不会创建或修改任何快照、不消耗版本号、不改变当前数据集。

```sh
curl -s 'http://127.0.0.1:8080/snapshots/diff?from=1&to=2'
# {"from":1,"to":2,"changes":[
#   {"key":"alpha","change":"modified","fields":{"from":{"n":1},"to":{"n":10}}},
#   {"key":"bravo","change":"unchanged","fields":null},
#   {"key":"charlie","change":"added","fields":{"n":3}}]}

curl -s 'http://127.0.0.1:8080/snapshots/diff?from=1&to=9'
# 404 {"error":"版本 9 不存在或尚未保存"}

curl -s 'http://127.0.0.1:8080/snapshots/diff?from=1'
# 400 {"error":"缺少必填查询参数 to"}
```

## 并发语义

写入、保存与读取在服务内部通过同一把锁串行化临界区：批次写入一次性提交，快照在保存时刻对数据集做完整深拷贝。保存版本时先在锁内完成原子落盘，落盘成功后才推进内存中的快照列表与版本号，因此任何一份已保存快照都与某次保存时刻的完整数据集一致，不会出现半批数据；写盘失败时服务对外保持保存前状态。
