# x-tunnel

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> ## 🪟 本分支目标（win7-compat）
>
> 本分支专为 **Windows 7 系统**客户端构建维护，**仅保留客户端代码**：
> - 使用 **Go 1.20** 构建（Go 1.21+ 已不再支持 Windows 7，故本分支锁固 Go 1.20）
> - **剔除 ECH（Encrypted Client Hello）特性**（依赖较新的 TLS/ECH 运行时支持，Win7 + Go 1.20 不具备）
> - **不包含服务端代码**（`server/` 目录已移除；服务端请使用 `main` 分支构建）
> - 本分支客户端可正常连接 `main` 分支构建的服务端
>
> 仅做 Win7 客户端兼容性维护；新功能与 ECH 相关增强在 `main` 分支开发。

**x-tunnel** 是一个基于 WebSocket 的高性能隧道代理系统,支持多通道连接池、SOCKS5 代理等功能.

## ✨ 特性

- 🚀 **高性能** - 多通道连接池,充分利用网络带宽
- 🔒 **安全加密** - 基于 TLS 1.3 的 WebSocket 连接
- 🌐 **SOCKS5 代理** - 完整的 SOCKS5 协议支持
- 🔄 **智能中转** - 支持中转节点自动选择和负载均衡
- 📊 **跨平台** - 支持 Linux、Windows、macOS,多架构编译
- 🧩 **易于集成** - 提供 Go 包,便于集成到其他项目

## 📖 架构

```
客户端                  服务端
  │                      │
  ├── SOCKS5 代理 ────→  │
  │                      │
  ├── 连接池 1 ────────→  ├── 连接池管理
  │   │  │               │      │
  │   └── WebSocket ───→  │      ├── 上行选择
  │                      │      │
  ├── 连接池 2 ────────→  │      ├── 下行选择
  │   │  │               │      │
  │   └── WebSocket ───→  │      └── 数据转发
  │                      │
  └── ... (更多通道) ──→  │
```

### 核心概念

- **多通道连接池**:客户端可以建立多个 WebSocket 连接,提高吞吐量和可靠性
- **通道选择机制**:
  - 初始阶段:客户端广播 TCP 连接请求到所有通道
  - 上行选择:第一个到达服务端的通道占用连接
  - 下行选择:客户端选择最快响应的通道作为下行
  - 数据传输:确定上下行后,使用单播传输数据
- **中转节点管理**:自动测速并选择最优中转节点

## 📦 安装

### 从源码编译

```bash
# 克隆仓库
git clone https://github.com/imshuai/x-tunnel.git
cd x-tunnel

# 编译（Linux/macOS）
./build.sh

# 编译（Windows CMD）
build.bat

# 编译（Windows PowerShell）
.\build.ps1
```

编译后的二进制文件将输出到 `bin/` 目录.

### 下载预编译版本

访问 [Releases 页面](https://github.com/imshuai/x-tunnel/releases) 下载对应平台的二进制文件.

## 🚀 快速开始

### 服务端

```bash
# 启动服务端（使用自签名证书）
./x-tunnel-server -l :8443 -token your_secret_token

# 使用自定义证书
./x-tunnel-server -l :8443 -token your_secret_token -cert server.crt -key server.key
```

### 客户端

```bash
# 基础使用
./x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://server:8443 -token your_secret_token -insecure -n 3

# 使用中转节点
./x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://server:8443 -token your_secret_token -insecure -n 3 -ip 1.1.1.1:443,8.8.8.8:443

# 测试 SOCKS5 代理
curl --socks5 127.0.0.1:1080 https://api.ipify.org
```

## 📚 作为库使用

x-tunnel 可以作为 Go 库集成到您的项目中.

### 导入包

```go
import (
    "github.com/imshuai/x-tunnel/client/pkg"
    "github.com/imshuai/x-tunnel/server/pkg"
    "github.com/imshuai/x-tunnel/common"
)
```

### 客户端示例

#### 基础客户端

```go
package main

import (
    "log"
    "time"

    "github.com/imshuai/x-tunnel/client/pkg"
)

func main() {
    // 创建配置
    config := &client.Config{
        ServerAddr:  "wss://your-server.com:8443",
        Token:       "your_secret_token",
        Connections: 3,                    // 3 个并发连接
        Insecure:    true,                 // 跳过证书验证（测试用）
        HandshakeTimeout: 10 * time.Second,
    }

    // 创建客户端
    c, err := client.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }

    // 启动客户端
    if err := c.Start(); err != nil {
        log.Fatal(err)
    }
    defer c.Shutdown()

    // 启动 SOCKS5 代理
    if err := c.ListenSOCKS5("127.0.0.1:1080"); err != nil {
        log.Fatal(err)
    }

    log.Println("SOCKS5 代理已启动,监听 127.0.0.1:1080")

    // 保持运行
    select {}
}
```

#### 使用中转节点

```go
package main

import (
    "log"

    "github.com/imshuai/x-tunnel/client/pkg"
)

func main() {
    config := &client.Config{
        ServerAddr:  "wss://your-server.com:8443",
        Token:       "your_secret_token",
        Connections: 3,
        Insecure:    true,
    }

    c, err := client.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }

    // 创建中转节点管理器
    relayMgr := client.NewRelayNodeManager()

    // 添加中转节点
    nodes := []string{
        "1.1.1.1:443",
        "8.8.8.8:443",
        "relay.example.com:443",
    }
    for _, node := range nodes {
        if err := relayMgr.AddNode(node, "443"); err != nil {
            log.Printf("添加节点失败: %v", err)
        }
    }

    // 启动中转节点管理器（后台测速）
    relayMgr.Start()
    defer relayMgr.Stop()

    // 启动客户端
    if err := c.Start(); err != nil {
        log.Fatal(err)
    }
    defer c.Shutdown()

    // 启动 SOCKS5 代理
    if err := c.ListenSOCKS5("127.0.0.1:1080"); err != nil {
        log.Fatal(err)
    }

    log.Println("客户端已启动,使用中转节点")

    select {}
}
```

#### 获取客户端统计信息

```go
// 获取统计信息
stats := c.Stats()
log.Printf("连接数: %d", stats.Connections)
log.Printf("活跃通道: %d", stats.ActiveChannels)
log.Printf("中转节点: %d", stats.RelayNodes)
log.Printf("发送字节: %d", stats.BytesSent)
log.Printf("接收字节: %d", stats.BytesReceived)
```

### 服务端示例

#### 基础服务端

```go
package main

import (
    "log"

    "github.com/imshuai/x-tunnel/server/pkg"
)

func main() {
    // 创建配置
    config := &server.Config{
        ListenAddr: ":8443",
        Token:      "your_secret_token",
        AutoCert:   true, // 自动生成自签名证书
    }

    // 创建服务端
    s, err := server.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    // 启动服务端
    if err := s.Start(); err != nil {
        log.Fatal(err)
    }
    defer s.Shutdown()

    log.Println("服务端已启动,监听 :8443")

    // 保持运行
    select {}
}
```

#### 使用自定义证书

```go
package main

import (
    "log"

    "github.com/imshuai/x-tunnel/server/pkg"
)

func main() {
    config := &server.Config{
        ListenAddr: ":8443",
        Token:      "your_secret_token",
        CertFile:   "/path/to/server.crt",
        KeyFile:    "/path/to/server.key",
    }

    s, err := server.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    if err := s.Start(); err != nil {
        log.Fatal(err)
    }
    defer s.Shutdown()

    log.Println("服务端已启动")

    select {}
}
```

#### 集成到现有 HTTP 服务器

```go
package main

import (
    "log"
    "net/http"

    "github.com/imshuai/x-tunnel/server/pkg"
)

func main() {
    // 创建配置
    config := &server.Config{
        ListenAddr: ":8443",
        Token:      "your_secret_token",
        AutoCert:   true,
    }

    // 创建服务端
    s, err := server.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    // 创建多路复用器
    mux := http.NewServeMux()

    // 注册 x-tunnel WebSocket 处理器
    mux.HandleFunc("/tunnel", s.Handler())

    // 注册其他 HTTP 处理器
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte("Hello, World!"))
    })

    // 启动 HTTPS 服务器
    srv := &http.Server{
        Addr:    ":8443",
        Handler: mux,
    }

    // 获取 TLS 证书
    cert, err := server.GenerateSelfSignedCert()
    if err != nil {
        log.Fatal(err)
    }

    srv.TLSConfig = &tls.Config{
        Certificates: []tls.Certificate{cert},
    }

    log.Println("服务器已启动")
    if err := srv.ListenAndServeTLS("", ""); err != nil {
        log.Fatal(err)
    }
}
```

#### 获取服务端统计信息

```go
// 获取统计信息
stats := s.Stats()
log.Printf("活跃连接: %d", stats.ActiveConnections)
log.Printf("活跃通道: %d", stats.ActiveChannels)
log.Printf("总连接数: %d", stats.TotalConnections)
log.Printf("发送字节: %d", stats.BytesSent)
log.Printf("接收字节: %d", stats.BytesReceived)
```

### 使用 Common 包

#### 消息编解码

```go
import "github.com/imshuai/x-tunnel/common"

// 编码消息
data := common.EncodeMessage(
    common.MsgTCPConnect,
    "conn-123",
    []byte("metadata"),
    []byte("payload"),
)

// 解码消息
msgType, connID, meta, payload, err := common.DecodeMessage(data)
if err != nil {
    log.Fatal(err)
}
```

#### IP 策略

```go
import "github.com/imshuai/x-tunnel/common"

// 解析 IP 策略
strategy, err := common.ParseIPStrategy("ipv4")
if err != nil {
    log.Fatal(err)
}

// 使用策略解析目标地址
target := "example.com:443"
resolved := common.ResolveWithStrategy(target, strategy)
log.Printf("解析结果: %s", resolved)
```

#### 错误处理

```go
import "github.com/imshuai/x-tunnel/common"

if err != nil {
    if common.IsNormalCloseError(err) {
        // 正常的连接关闭,不需要处理
        log.Println("连接正常关闭")
        return
    }
    // 其他错误需要处理
    log.Printf("错误: %v", err)
}
```

## ⚙️ 配置说明

### 客户端配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-l` | SOCKS5 监听地址 | 必填 |
| `-f` | WebSocket 服务器地址 | 必填 |
| `-token` | 认证令牌 | 必填 |
| `-n` | 并发连接数 | 3 |
| `-insecure` | 跳过 TLS 证书验证 | false |
| `-ip` | 中转节点列表（逗号分隔） | - |
| `-timeout` | 握手超时（秒） | 10 |

### 服务端配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-l` | HTTPS 监听地址 | 必填 |
| `-t` / `-token` | 认证令牌 | 必填 |
| `-cert` | TLS 证书文件 | - |
| `-key` | TLS 私钥文件 | - |

## 🔧 高级用法

### IP 策略

x-tunnel 支持 IPv4/IPv6 地址解析策略:

```bash
# 仅使用 IPv4
./x-tunnel-client ... -ip-strategy ipv4

# 仅使用 IPv6
./x-tunnel-client ... -ip-strategy ipv6

# IPv4 优先
./x-tunnel-client ... -ip-strategy ipv4-ipv6

# IPv6 优先
./x-tunnel-client ... -ip-strategy ipv6-ipv4
```

### 中转节点

使用中转节点可以绕过网络限制或提高连接质量:

```bash
# 指定多个中转节点
./x-tunnel-client ... \
  -ip 1.1.1.1:443,8.8.8.8:443,relay.example.com:443

# 支持域名（会自动解析为 IP）
./x-tunnel-client ... \
  -ip relay1.example.com,relay2.example.com
```

中转节点管理器会:
- 定期测速（30 秒间隔）
- 根据延迟和成功率评分
- 自动选择最优节点

## 🛠️ 开发

### 项目结构

```
x-tunnel/
├── client/           # 客户端模块
│   ├── cmd/         # 客户端入口
│   ├── pkg/         # 客户端包
│   └── examples/    # 客户端示例
├── server/          # 服务端模块
│   ├── cmd/         # 服务端入口
│   ├── pkg/         # 服务端包
│   └── examples/    # 服务端示例
├── common/          # 公共包
│   ├── protocol.go  # 协议定义
│   ├── ip_strategy.go  # IP 策略
│   └── errors.go    # 错误处理
├── build.sh         # Linux/macOS 构建脚本
├── build.bat        # Windows CMD 构建脚本
└── build.ps1        # PowerShell 构建脚本
```

### 编译

```bash
# 清理并重新编译
./build.sh --clean

# 显示详细编译命令
./build.sh --verbose

# 查看帮助
./build.sh --help
```

## ❓ 常见问题

### Q: 如何在生产环境部署？

A: 建议使用:
1. 正式的 TLS 证书（如 Let's Encrypt）
2. `-insecure=false` 启用证书验证
3. 使用 systemd 或其他进程管理器
4. 配置日志轮转
5. 监控服务状态

### Q: 连接失败怎么办？

A: 检查:
1. 服务端是否正常运行
2. Token 是否正确
3. 防火墙是否开放端口
4. TLS 证书是否有效（使用 `-insecure` 跳过验证进行测试）
5. 网络连接是否正常

### Q: 如何提高性能？

A: 可以:
1. 增加并发连接数（`-n` 参数）
2. 使用中转节点选择最优路径
3. 优化服务端配置

### Q: 支持哪些代理协议？

A: 目前支持 SOCKS5（RFC 1928）,包括:
- CONNECT 命令（TCP）
- UDP ASSOCIATE
- 用户名/密码认证

计划支持 HTTP Proxy 协议.

## 📄 License

[MIT License](LICENSE)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 🔗 相关链接

- [GitHub 仓库](https://github.com/imshuai/x-tunnel)
- [Issue Tracker](https://github.com/imshuai/x-tunnel/issues)
- [文档](https://github.com/imshuai/x-tunnel/wiki)
