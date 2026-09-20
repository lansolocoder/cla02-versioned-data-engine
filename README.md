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

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version
```
