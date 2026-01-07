# x-tunnel 开发计划

## 项目概述
x-tunnel 是一个基于 WebSocket 的隧道代理系统，支持多通道连接池、SOCKS5 代理、ECH (Encrypted Client Hello) 等功能。

## 已完成

### 1. 代码重构 (Commit: 29fa71a)
- 将代码分离为 client/server 模块，使用 build tags
- 提取公共代码到 `common.go`, `protocol.go`
- 实现 IP 策略选择 (`ip_strategy.go`)
- 服务端连接池管理 (`server_pool.go`)
- 修复 TLS 握手超时：添加广播数据去重逻辑
- 优化日志：添加通道连接和访问日志，移除调试日志
- 添加 `.gitignore`

### 2. 连接关闭优化 (Commit: 8fe6165)
**服务端修复:**
- 添加 `closed` 标志防止 `ServerConnState` 重复注销
- 过滤正常的连接关闭错误（use of closed network connection, tls: bad record MAC, websocket: close sent 等）
- 修复 `writeWorker` 在写入失败时立即退出

**客户端优雅关闭:**
- 添加 `context/cancel` 到 `ECHPool` 用于通知 goroutine 退出
- 实现 `Shutdown()` 方法：发送 WebSocket Close Frame (1000) 后关闭连接
- 捕获 SIGINT/SIGTERM 信号，触发优雅关闭
- 修改 `dialAndServe` 和 `writeWorker` 响应 context 取消

**公共代码优化:**
- 扩展 `isNormalCloseError` 支持字符串匹配
- 添加 TLS 和 WebSocket 关闭错误的检测

## 架构说明

### 文件结构
```
x-tunnel/
├── protocol.go        # 消息类型定义、编码解码（client/server 共享）
├── common.go          # 公共函数（client/server 共享）
├── ip_strategy.go     # IP 策略解析（client/server 共享）
├── x-tunnel-server.go # 服务端入口（仅 server build）
├── server_pool.go     # 服务端连接池和消息处理（仅 server build）
├── x-tunnel-client.go # 客户端入口和连接池（仅 client build）
└── .gitignore         # Git 忽略文件
```

### 消息类型 (protocol.go)
- `MsgTCPConnect`: TCP 连接请求
- `MsgTCPData`: TCP 数据传输
- `MsgTCPClose`: TCP 连接关闭
- `MsgSelectUplink`: 服务端选择上行通道
- `MsgSelectDownlink`: 客户端选择下行通道
- `MsgConnStatus`: 连接状态通知
- `MsgUDPConnect`: UDP 连接请求
- `MsgUDPData`: UDP 数据传输
- `MsgUDPClose`: UDP 连接关闭

### 通道选择机制
1. **初始阶段**: 客户端广播 `MsgTCPConnect` 到所有通道
2. **上行选择**: 第一个到达服务端的通道占用连接，发送 `MsgSelectUplink`
3. **下行选择**: 最快收到 `MsgSelectUplink` 的通道作为下行通道
4. **数据传输**: 确定上下行后，使用单播传输数据

### 连接关闭流程
**正常关闭:**
1. 客户端捕获 SIGINT/SIGTERM
2. 调用 `Shutdown()` 发送 Close Frame (1000)
3. 关闭所有写队列
4. 服务端收到 Close Frame，清理连接
5. 输出日志：`通道 X 已断开`

**异常处理:**
- 过滤正常的连接关闭错误，避免输出无意义的日志
- 使用 `closed` 标志防止重复注销

## 编译
```bash
# 服务端
go build -tags server -o x-tunnel-server x-tunnel-server.go server_pool.go ip_strategy.go common.go protocol.go

# 客户端
go build -tags client -o x-tunnel-client x-tunnel-client.go ip_strategy.go common.go protocol.go
```

## 运行示例
```bash
# 服务端
./x-tunnel-server -l :8443 -t your_token

# 客户端
./x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://server:8443 -token your_token -insecure -n 3
```

## 待优化项
- [ ] 添加连接数限制
- [ ] 添加流量统计
- [ ] 添加配置文件支持
- [ ] 优化 ECH 配置刷新机制
- [ ] 添加连接超时控制
- [ ] 支持更多代理协议 (HTTP Proxy)
