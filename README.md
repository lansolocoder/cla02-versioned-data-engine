# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及内存中的版本化数据集：通过提交（commit）写入变更，通过快照（snapshot）读取任意历史版本。数据仅保存在内存中，进程重启即丢失。

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
| `POST /datasets/{name}/commits` | `201 {"dataset":"...","version":N,"request_id":"..."}` |
| `GET /datasets/{name}/snapshots/{version}` | `200 {"dataset":"...","version":N,"records":[{"key":"...","value":...}]}` |

## 提交变更

```sh
curl -X POST http://127.0.0.1:8080/datasets/users/commits \
  -H 'content-type: application/json' \
  -d '{"base":null,"request_id":"req-1","upserts":{"alice":{"age":30}},"deletes":[]}'
```

- `base`：首次提交为 `null`，之后必须为当前版本号，否则 `409 stale_base`。
- `request_id`：非空，按数据集幂等。相同内容重放返回首次响应且不新增版本（即使 base 已过期）；内容不同返回 `409 request_id_conflict`。
- `upserts`：非空 key 到任意 JSON value 的映射；`deletes`：key 数组。两者不能同时为空。
- 空名称/key、重复 key、同一 key 同时写删、字段类型错误、畸形 JSON 均返回 `400`。
- 每个数据集版本从 1 递增；失败的提交不产生版本或改动；相同 base 的并发提交只有一个成功。

## 读取快照

`version` 为版本号或 `latest`。记录按 key 的 Unicode 码点顺序排列；历史快照不可变。未知数据集或版本返回 `404`。

```sh
curl http://127.0.0.1:8080/datasets/users/snapshots/latest
curl http://127.0.0.1:8080/datasets/users/snapshots/1
```

所有错误响应均为 `{"code":"...","message":"..."}`，`code` 稳定可读。
