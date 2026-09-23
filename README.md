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

## 反向模式

反向模式下流量方向与正向相反：客户端 `-l` 的值不再开本地监听，而是发给服务端由其开监听；
外部连服务端监听端口的流量经隧道由**客户端**出网。适用于服务端出网受限（客户端有出网）
或想把客户端侧网络出口提供给服务端侧使用的场景。

```bash
# 服务端（零配置，开关与监听值均由客户端决定）
x-tunnel-server -l :443 -token <TOKEN>

# 客户端：-r 启用反向，-l 的值改为传给服务端（绑定地址由服务端解释）
x-tunnel-client -r -l socks5://0.0.0.0:30000 -f wss://<server>:443 -token <TOKEN> -n 3

# 外部经服务端 30000 端口走 SOCKS5，流量由客户端出网
curl --socks5-hostname <server>:30000 https://www.google.com/generate_204
```

行为要点：

- 服务端**零新增配置**：开关由客户端 `-r` 决定，监听值由客户端 `-l` 决定；仅
  `-max-reverse-listeners`（默认 3）限制每客户端监听器数量。
- 监听生命周期跟随客户端连接：全部通道断开即注销释放端口，重连后按 `-l` 幂等自动恢复。
- 支持 `socks5://[user:pass@]host:port` 鉴权与多个 `-l` 值；启动首轮注册全败 fatal 退出，
  运行中重连注册失败仅告警不退出。
- ⚠️ v1 限制：反向目标拨号在**客户端**执行且未过滤私网地址（客户端等于把内网出口交给
  服务端侧使用，请仅在信任服务端时启用）；UDP 反向暂不支持。
- 配合 `-hotpair`：预热通道对同样加速反向连接（健康维护统一由客户端负责），拨号期零选路消息。

## 客户端常用参数

```
-l           监听地址（socks5://[user:pass@]host:port，多个逗号分隔）
             -r/-reverse 时改为发送给服务端由其开监听（详见反向模式一节）
-f           服务端地址（wss://host:port/path）
-token       身份验证令牌（WebSocket Subprotocol）
-ip          中转节点（逗号分隔，支持域名；测速超阈自动直连）
-n           每个 IP 的 WebSocket 连接数（默认 3）
-block       UDP 拦截端口（默认 443）
-ech         ECH 查询域名；-fallback 禁用 ECH 回落普通 TLS
-insecure    跳过证书校验（自动禁用 ECH）
-hotpair     启用 Hot Channel Pair 双端热表降低首帧延迟（正反双模式通用）
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
-max-reverse-listeners 每客户端最大反向监听器数（默认 3，防滥用）
```

## 协议与库

8 字节二进制协议（connID + msgType）、多通道下行竞争、Hot Pair 双端热表、反向模式、
UDP ASSOCIATE、背压控制等实现细节见 [`xtunnel`](https://github.com/v2up-32mb/xtunnel) 库仓 README；
SOCKS5/HTTP 本地代理服务器由 [`xshared`](https://github.com/v2up-32mb/xshared) 提供。

## 开发

```bash
go build -o x-tunnel-client ./client/cmd/x-tunnel-client
go build -o x-tunnel-server ./server/cmd/x-tunnel-server
go test ./... -race
```

## License

见上游约定；贡献请提 Issue/PR。
