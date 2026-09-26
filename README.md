# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及集合级别的版本化存储链路：批量写入结构化记录、冻结不可变快照、按版本回读并比较两版差异。状态保存在进程内存中，重启后清空。

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
| `PUT /collections/{collection}/records` | `{"applied":N}` |
| `POST /collections/{collection}/snapshots` | `{"version":V}` |
| `GET /collections/{collection}/snapshots` | `{"versions":[{"version":V,"recordCount":M}]}` |
| `GET /collections/{collection}/snapshots/{version}` | `{"version":V,"records":[{"id","data"}]}` |
| `GET /collections/{collection}/snapshots/{from}/diff/{to}` | `{"from","to","added","removed","changed"}` |

集合在首次成功写入时自动出现。

## 写入记录

`PUT .../records` 接收 `{"records":[{"id":...,"data":{...}}]}`，整批原子生效：

- `id` 必须为非空字符串，且批内不可重复；`data` 必须是 JSON 对象。
- 任一记录不合法时整批拒绝，集合保持原样，返回 `400 {"error":{"code":"invalid_records"}}`。
- 全部合法时按 id 覆盖写入，批中未提及的记录保留；返回 `{"applied":N}`。

## 快照

- `POST .../snapshots` 冻结当前全部记录，版本号从 1 起按成功顺序递增。冻结内容不受之后写入影响；被拒绝的写入不消耗版本号。
- `GET .../snapshots/{version}` 的 records 按 id 升序；版本不存在或集合从未保存快照返回 `404 {"error":{"code":"snapshot_not_found"}}`。
- `GET .../snapshots/{from}/diff/{to}` 要求 `from <= to` 且两端快照都存在：`from > to` 返回 `400 {"error":{"code":"invalid_snapshot_range"}}`，端点不存在返回上述 404。
- 差异结果中 `added` / `removed` / `changed` 的 id 均按升序排列，分别表示只在 to、只在 from、两版都有但冻结内容不同。

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version

curl -X PUT http://127.0.0.1:8080/collections/demo/records \
  -d '{"records":[{"id":"a","data":{"v":1}},{"id":"b","data":{"v":2}}]}'
curl -X POST http://127.0.0.1:8080/collections/demo/snapshots
curl http://127.0.0.1:8080/collections/demo/snapshots/1
curl http://127.0.0.1:8080/collections/demo/snapshots/1/diff/1
```
