# AGENTS.md — x-tunnel (xtunnel-cli) 开发计划

> 说明：本文件采用通用的 `AGENTS.md` 命名（取代 Claude 专有命名 `CLAUDE.md`），
> 任何 agent / 编辑器 / 工具均按此约定读取。

# x-tunnel 开发计划

## 项目概述
x-tunnel 是一个基于 WebSocket 的隧道代理系统,支持多通道连接池、SOCKS5 代理、ECH (Encrypted Client Hello) 等功能.

## 已完成

### 1. 代码重构 (Commit: 29fa71a)
- 将代码分离为 client/server 模块,使用 build tags
- 提取公共代码到 `common.go`, `protocol.go`
- 实现 IP 策略选择 (`ip_strategy.go`)
- 服务端连接池管理 (`server_pool.go`)
- 修复 TLS 握手超时:添加广播数据去重逻辑
- 优化日志:添加通道连接和访问日志,移除调试日志
- 添加 `.gitignore`

### 2. 连接关闭优化 (Commit: 8fe6165)
**服务端修复:**
- 添加 `closed` 标志防止 `ServerConnState` 重复注销
- 过滤正常的连接关闭错误（use of closed network connection, tls: bad record MAC, websocket: close sent 等）
- 修复 `writeWorker` 在写入失败时立即退出

**客户端优雅关闭:**
- 添加 `context/cancel` 到 `ECHPool` 用于通知 goroutine 退出
- 实现 `Shutdown()` 方法:发送 WebSocket Close Frame (1000) 后关闭连接
- 捕获 SIGINT/SIGTERM 信号,触发优雅关闭
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
├── x-tunnel-client.go # 客户端入口（仅 client build）
├── client_pool.go     # 客户端连接池管理（仅 client build）
├── client_dial.go     # WebSocket 连接和 ECH 配置（仅 client build）
├── client_socks5.go   # SOCKS5 代理实现（仅 client build）
├── relay_manager.go   # 中转节点管理器（仅 client build）
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
2. **上行选择**: 第一个到达服务端的通道占用连接,发送 `MsgSelectUplink`
3. **下行选择**: 最快收到 `MsgSelectUplink` 的通道作为下行通道
4. **数据传输**: 确定上下行后,使用单播传输数据

### 连接关闭流程
**正常关闭:**
1. 客户端捕获 SIGINT/SIGTERM
2. 调用 `Shutdown()` 发送 Close Frame (1000)
3. 关闭所有写队列
4. 服务端收到 Close Frame,清理连接
5. 输出日志:`通道 X 已断开`

**异常处理:**
- 过滤正常的连接关闭错误,避免输出无意义的日志
- 使用 `closed` 标志防止重复注销

### Ping/Pong 心跳机制
**客户端:**
- 每 5 秒发送一次 Ping 消息
- 设置 PongHandler 自动响应并重置读超时
- 读超时:15 秒

**服务端:**
- 每 5 秒发送一次 Ping 消息
- 设置 PingHandler 自动响应 Pong
- 读超时:15 秒

## 编译
```bash
# 服务端
go build -tags server -o x-tunnel-server x-tunnel-server.go server_pool.go ip_strategy.go common.go protocol.go server_cert.go

# 客户端
go build -tags client -o x-tunnel-client x-tunnel-client.go client_*.go ip_strategy.go common.go protocol.go relay_manager.go
```

## 运行示例
```bash
# 服务端
./x-tunnel-server -l :8443 -t your_token

# 客户端（基础）
./x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://server:8443 -token your_token -insecure -n 3

# 客户端（使用中转节点）
./x-tunnel-client -l socks5://127.0.0.1:1080 -f wss://server:8443 -token your_token -insecure -n 3 -ip 1.1.1.1:443,8.8.8.8:443,relay.example.com:443
```

### 3. 中转节点管理器 (Commit: 252c4e0)
**RelayNodeManager 实现:**
- 实现中转节点解析,支持多种格式（IP, IP:PORT, 域名, 域名:PORT）
- DNS 解析获取节点 IP 列表
- TCP 连接测速（轻量级测试方式）
- 节点评分算法:综合考虑延迟、成功率和稳定性
- 定期后台测速任务（30秒间隔）
- 与现有 IP 策略兼容
- 节点选择支持负载均衡

**客户端集成:**
- 添加 `-ip` 命令行参数支持中转节点配置
- 在 ECHPool 中集成 RelayNodeManager
- 在连接建立时使用最佳节点
- 优雅关闭时停止中转节点管理器

## 待优化项
- [x] 添加中转节点管理器
- [ ] 添加连接数限制
- [ ] 添加流量统计
- [ ] 添加配置文件支持
- [ ] 优化 ECH 配置刷新机制
- [ ] 添加连接超时控制
- [ ] 支持更多代理协议 (HTTP Proxy)
- [ ] 预绑定竞速演进(B 方案):新增显式 begin 消息替代预绑定状态短存活定时窗口(prebindStateTTL)。
      客户端广播前先通知服务端"即将开始 hotpair 建立",服务端 armed;首帧 prebind 定上行并
      广播 MsgSelectUplink;其余帧在该 connID 状态下被忽略;轮次结束(客户端 done 或超时)
      立即注销。目标:轮次边界显式化、低延迟链路上不再依赖 5s 定时窗口、广播次数确定性为每轮 1 次。
      注意:属协议变更,需同步 win7-compat 客户端、上游 xtunnel 模块(升版/发版)与服务端,旧端需兼容兑底。
- [ ] 服务端核心库化(C 方案):将 server/pkg 迁入上游 xtunnel 库(如 xtunnel/server 子包)。✅ 已完成前置:
      服务端核心日志已收敛为注入钩子(server.SetLogf,默认静默,50 处 srvLog 带等级/模块),壳已统一
      [等级][模块] 风格;迁移时仅需改 import 与包名,接入就绪。
      目标:服务端核心可复用、日志管理统一由壳负责、protocol+服务端核心同版本同步发版。
      代价:全量迁移改 import,三方回归;与客户端核心同库导致发版耦合。
      建议与 B 方案、协议变更一起排入 v0.3.0 演进批次。
- [ ] 结构化领域事件(方向 2):在日志事件基础上提供结构化字段(连接事件: ID/Target/通道/State/流量,
      HotPair 事件: 键/上下行/promoted/回退,池事件: 通道上下线/背压档位),壳可统计/过滤而不解析字符串;
      保留字符串渲染兼容(事件可派生 Message),既有 LogEvent 契约不破坏;推荐排 v0.3.1,可与 B 并行。
- 完整 v0.3.0 编排(顺序/协议草案/验收/发布红线)见上游 `xtunnel/AGENTS.md`「v0.3.0 规划」,新会话开工先读那里。

## RelayNodeManager 实现说明

### 数据结构
```go
// RelayNode 表示一个中继节点
type RelayNode struct {
    ID          string        // 节点ID
    Address     string        // 节点地址
    IP          string        // 解析后的IP
    Score       float64       // 节点评分
    LastTest    time.Time     // 最后测试时间
    Latency     time.Duration // 延迟
    SuccessRate float64       // 成功率
    Weight      float64       // 权重（用于负载均衡）
}

// RelayNodeManager 管理所有中转节点
type RelayNodeManager struct {
    nodes     []*RelayNode
    mu        sync.RWMutex
    testTimer *time.Ticker
    ctx       context.Context
    cancel    context.CancelFunc
}
```

### 功能特性
1. **多格式节点地址解析**: 支持 IP, IP:PORT, 域名, 域名:PORT
2. **DNS 自动解析**: 域名自动解析为 IP 列表（使用系统默认 DNS）
3. **TCP 速度测试**: 使用轻量级 TCP 连接测速
4. **评分算法**: 综合考虑延迟（70%）和成功率（30%）
5. **定期测速**: 后台每 30 秒测试一次所有节点
6. **负载均衡**: 根据节点评分选择最优节点

### 命令行参数
- `-ip`: 中转节点地址列表,支持多种格式,多个用逗号分隔
  - 示例: `-ip 1.1.1.1:443,8.8.8.8:443,relay.example.com:443`

### 评分算法
节点评分 = (1 - 归一化延迟) × 0.7 + 成功率 × 0.3 × 衰减因子

其中:
- 归一化延迟 = min(延迟, 5秒) / 5秒
- 成功率 = 测试成功时为 1.0,失败为 0.0
- 衰减因子 = 1 / (1 + 测试后小时数 × 0.1)（超过 1 小时后衰减）

### 集成点
1. **x-tunnel-client.go**: 使用 `-ip` 参数加载中转节点到 relayManager
2. **client_pool.go**: Start() 中启动 RelayNodeManager,Shutdown() 中停止
3. **client_pool.go**: 在 Start() 中使用 relayManager.SelectBestNode() 获取最佳节点 IP
