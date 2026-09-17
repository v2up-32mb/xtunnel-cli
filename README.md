# xtunnel-cli

x-tunnel 多通道 WebSocket 隧道的命令行实现（客户端 + 服务端）。协议栈在
[`xtunnel`](https://github.com/v2up-32mb/xtunnel) 库，本仓只保留 CLI 壳
（入口、配置文件、服务端）。

## 安装

从 [Releases](https://github.com/v2up-32mb/xtunnel-cli/releases) 下载对应平台二进制
（每平台含 `x-tunnel-client` 与 `x-tunnel-server`，linux/darwin/windows × amd64/arm64，纯静态）。

## 快速开始

服务端（自签证书自动生成）：

```bash
x-tunnel-server -l :443 -token <TOKEN>
```

客户端：

```bash
x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://<server>:443 -token <TOKEN> -n 3
curl --socks5-hostname 127.0.0.1:1080 https://www.google.com/generate_204
```

## 客户端常用参数

```
-l           监听地址（socks5://[user:pass@]host:port，多个逗号分隔）
-f           服务端地址（wss://host:port/path）
-token       身份验证令牌（WebSocket Subprotocol）
-ip          中转节点（逗号分隔，支持域名；测速超阈自动直连）
-n           每个 IP 的 WebSocket 连接数（默认 3）
-block       UDP 拦截端口（默认 443）
-ech         ECH 查询域名；-fallback 禁用 ECH 回落普通 TLS
-insecure    跳过证书校验（自动禁用 ECH）
-hotpair     启用 Hot Channel Pair 降低首帧延迟
-dns         查询 ECH 公钥的 DNS 服务器（DoH 或 UDP）
-max-socks5-conns  SOCKS5 最大并发连接数（0 无限制）
--bypass-private/--bypass-geoip-cn/--bypass-geosite-cn  路由绕过开关，配合 --bypass-rules 自定义规则
--geo-ip/--geo-site  指定 geoip.dat/geosite.dat 路径，留空自动探测可执行文件同目录
```

完整参数见 `-h`；JSON 配置文件用 `-config`（CLI 参数优先）。IPv4/IPv6 策略
`-ips 4|6|4,6|6,4`。

## 服务端常用参数

```
-l                    监听地址（默认 :8443）
-token                身份验证令牌
-cert / -key          TLS 证书（不指定则自动生成自签）
-max-client-channels  每客户端最大通道数
-backpressure-limit   全局队列背压阈值（默认 32MB）
```

## 协议与库

8 字节二进制协议（connID + msgType）、多通道下行竞争、Hot Pair、UDP ASSOCIATE、
背压控制等实现细节见 [`xtunnel`](https://github.com/v2up-32mb/xtunnel) 库仓 README；
SOCKS5/HTTP 本地代理服务器由 [`xshared`](https://github.com/v2up-32mb/xshared) 提供。

## 开发

```bash
go build -o x-tunnel-client ./client/cmd/x-tunnel-client
go build -o x-tunnel-server ./server/cmd/x-tunnel-server
go test ./... -race
```

## License

见上游约定；贡献请提 Issue/PR。
