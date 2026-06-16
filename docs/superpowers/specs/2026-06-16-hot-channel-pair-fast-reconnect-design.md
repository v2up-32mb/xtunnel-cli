# x-tunnel Hot Channel Pair 与快速重连设计

## 1. 背景与目标

x-tunnel 当前基于 WebSocket 多通道连接池，采用“请求到达后临时广播 `MsgTCPConnect`、服务端选上行、客户端选下行”的竞争机制。该机制在 SOCKS5/HTTP 代理短连接场景下会叠加以下延迟：

1. TLS + WebSocket 握手（若通道未就绪）。
2. 首次通道竞争的一次或多次 RTT。
3. 当前节点失败或通道断开时，固定 3s 基数指数退避重连。

本设计引入 **Hot Channel Pair（热通道对）** 与 **快速重连/节点切换**，目标：

- **降低首次响应延迟**：代理请求到达时复用已预热的最优上下行通道对。
- **加强链路稳定性**：动态重评链路质量并刷新 Hot Pair；失败时快速切换节点/通道。
- **保持架构一致**：复用现有消息类型与通道选择逻辑，新增可选协议字段。

## 2. 核心概念

### 2.1 Hot Channel Pair

客户端维护若干已预先完成上下行绑定的通道对。每个 Pair 包含：

```go
type HotChannelPair struct {
    ID           string
    UplinkChID   int
    DownlinkChID int
    wsUplink     *websocket.Conn
    wsDownlink   *websocket.Conn
    ready        bool
    createdAt    time.Time
    refs         int32 // 当前正在使用该 Pair 的代理请求数
}
```

代理请求到达时，直接从当前主 Pair 中取出上下行通道发送首个消息，不再临时握手。

### 2.2 预绑定（Prebind）

预绑定通过一次虚拟的 TCP 连接请求完成，沿用现有广播竞争逻辑：

1. 客户端生成虚拟 `connID`（如 `prebind-<uuid>`）。
2. 在 `MsgTCPConnect` 的 meta 中设置 `PrebindFlag`。
3. 调用现有 `broadcastWrite` 广播到所有活跃通道。
4. 服务端按现有逻辑选择上行通道并广播 `MsgSelectUplink`。
5. 客户端按现有逻辑选择下行通道并回传 `MsgSelectDownlink`。
6. 服务端识别 `PrebindFlag`，不真正连接目标地址，只记录 Pair 并回复 `MsgConnStatus(StatusOK)`。

### 2.3 Pair 池与刷新

- 默认维护 `N=2` 个 Hot Pair（可配置）。
- 后台 **Pair Warmer** 周期性评估链路质量：
  - 每次 `RelayNodeManager` 测速完成或固定 30s 间隔触发。
  - 若当前 Pair 评分仍最优，继续使用。
  - 若发现更优链路，启动新 Pair 构建。
- 新 Pair 就绪后成为“主 Pair”；旧 Pair 进入 `draining`：
  - 新请求只使用新 Pair。
  - 旧 Pair 的 `refs` 归零后安全关闭其独占通道。
  - 若旧 Pair 的某条通道也被新 Pair 复用，则该通道引用计数 +1，旧 Pair 释放时不关闭它。

## 3. 协议变更

### 3.1 新增消息类型（common/protocol.go）

```go
const (
    MsgPrebindRequest MessageType = iota + 0x10 // 保留给未来使用
    MsgChannelReset                              // 服务端通知客户端某通道需要重置
)
```

`MsgChannelReset` 用于服务端主动通知客户端某条通道异常，客户端立即废弃包含该通道的所有 Pair 并重建。

### 3.2 MsgTCPConnect meta 扩展

当前 meta 结构：

```
[0]        IPStrategy
[1..n]     target 字符串
```

扩展为：

```
[0]        IPStrategy
[1]        Flags（bit 0: PrebindFlag）
[2..n]     target 字符串
```

服务端解析时：

- 若 `PrebindFlag` 置位，不调用 `connectTarget`，仅完成通道选择并记录 Pair。
- 若对端未识别 `Flags`，默认按旧语义处理（向后兼容风险低，因为旧版本会忽略未知 bit）。

## 4. 服务端改动

### 4.1 ServerConnState 扩展

```go
type ServerConnState struct {
    // ... 现有字段
    isPrebind bool
}
```

### 4.2 handleTCPConnect 调整

```go
func (p *serverPool) handleTCPConnect(chID int, connID string, meta []byte) {
    strategy, flags, target := parseConnectMeta(meta)
    isPrebind := flags&PrebindFlag != 0

    st := &ServerConnState{
        // ...
        isPrebind: isPrebind,
    }

    if !isPrebind {
        // 真实目标连接
        conn, err := p.connectTarget(target, strategy)
        if err != nil {
            // ...
        }
        st.targetConn = conn
    }

    p.registerUplink(st, chID)
    p.broadcastSelectUplink(st)
}
```

### 4.3 通道引用计数

```go
type ServerWSConn struct {
    // ... 现有字段
    hotPairRef int32 // 被多少个 Hot Pair 引用
}
```

- 通道被纳入某个 Hot Pair 时 `atomic.AddInt32(&hotPairRef, 1)`。
- Pair 释放时 `atomic.AddInt32(&hotPairRef, -1)`。
- `cleanupChannel` 在关闭前检查 `hotPairRef == 0`，否则仅标记为 `pendingClose`，待 ref 归零后再关闭。

### 4.4 MsgChannelReset 处理

服务端在检测到某通道异常（连续写失败、心跳超时）时：

```go
p.broadcastWriteToClient(clientID, websocket.BinaryMessage,
    common.EncodeMessage(common.MsgChannelReset, "", meta, nil))
```

meta 携带异常 `chID`。

## 5. 客户端改动

### 5.1 新增 Pair Warmer

新增文件 `client/pkg/pair_warmer.go`：

```go
type PairWarmer struct {
    pool      *clientPool
    mu        sync.RWMutex
    pairs     []*HotChannelPair
    primary   *HotChannelPair
    config    PairWarmerConfig
    ctx       context.Context
    cancel    context.CancelFunc
}

type PairWarmerConfig struct {
    PairCount          int
    RefreshInterval    time.Duration
    PrebindTimeout     time.Duration
}
```

职责：

- 启动时构建第一批 Hot Pair。
- 监听测速结果或定时器，触发 Pair 刷新。
- 管理 Pair 生命周期（ready / draining / closed）。

### 5.2 代理请求复用 Hot Pair

`RegisterAndBroadcastTCP` 改为：

```go
func (p *clientPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
    pair := p.pairWarmer.AcquirePrimary()
    if pair == nil || !pair.ready {
        // 退化到原广播逻辑
        p.registerAndBroadcastFallback(connID, target, first, tcpConn, reqType)
        return
    }

    // 直接通过主 Pair 的上行通道发送 MsgTCPConnect
    p.registerConnection(connID, target, pair, tcpConn, reqType)
    msg := common.EncodeMessage(common.MsgTCPConnect, connID,
        buildConnectMeta(p.config.IPStrategy, 0, target), first)
    _ = p.asyncWriteDirect(pair.UplinkChID, websocket.BinaryMessage, msg)
}
```

### 5.3 通道引用计数

客户端同样为每个 WebSocket 通道维护 `hotPairRef`：

```go
type clientChannelState struct {
    ws         *websocket.Conn
    hotPairRef int32
}
```

实现与 4.3 对称。

### 5.4 快速重连与节点切换（方案 3）

在 `dialAndServe` 中：

- 写入/读取失败时，先进入 **fast retry** 模式：
  - 500ms 内最多 2 次快速重试。
  - 若 fast retry 成功，标记节点恢复。
- fast retry 失败后，再进入现有指数退避。
- 中转节点失败时，立即调用 `SelectNodeExcluding(lastIP)` 尝试新节点。
- 收到 `MsgChannelReset` 时，立即关闭对应通道并触发 Pair 重建。

### 5.5 动态测速频率

`RelayNodeManager` 增加网络质量指数：

```go
type RelayNodeManager struct {
    // ...
    healthScore int32 // 0-100，基于最近失败率
}
```

- `healthScore < 30`：测速间隔缩短到 10s。
- `30 <= healthScore < 70`：默认 30s。
- `healthScore >= 70`：测速间隔延长到 60s。

## 6. 数据流

```
启动阶段:
  PairWarmer 构建 Pair-1, Pair-2
       │
       ▼
  预绑定握手 (复用 MsgTCPConnect/MsgSelectUplink/MsgSelectDownlink)
       │
       ▼
  Pair 就绪，进入主/备池

代理请求阶段:
  SOCKS5/HTTP 请求 ──► AcquirePrimary() ──► 直接经 Pair 上行通道发送 MsgTCPConnect
                                              │
                                              ▼
                                         服务端经 Pair 下行通道回复 StatusOK
                                              │
                                              ▼
                                         代理连接建立，继续数据转发

刷新阶段:
  测速完成 / 定时器触发 ──► 评估当前 Pair 质量
       │
       ├── 仍最优 ──► 继续
       │
       └── 发现更优链路 ──► 构建新 Pair
                              │
                              ▼
                       新 Pair 成为主 Pair
                       旧 Pair refs=0 后关闭（注意引用计数避免误关复用通道）
```

## 7. 配置项

新增客户端配置：

```go
type Config struct {
    // ... 现有字段
    EnableHotPair      bool          // 是否启用热通道对
    HotPairCount       int           // Hot Pair 数量，默认 2
    HotPairRefreshInterval time.Duration // 默认 30s
    FastRetryAttempts  int           // 快速重试次数，默认 2
    FastRetryWindow    time.Duration // 快速重试窗口，默认 500ms
}
```

命令行参数：

```bash
-hotpair            # 启用 Hot Pair
-hotpair-count 2    # Pair 数量
-hotpair-refresh 30s
-fast-retry 2       # 快速重试次数
```

## 8. 错误处理

- 预绑定失败：降级到原有广播逻辑，不影响功能。
- Hot Pair 不可用：退化到原有广播逻辑。
- 新旧 Pair 通道复用：通过引用计数保证不会误关仍在使用的通道。
- 服务端/客户端版本不一致：旧版本忽略 `Flags` 和 `MsgChannelReset`，行为与当前一致。

## 9. 测试策略

1. **单元测试**（client/pkg/pair_warmer_test.go）：
   - Pair 构建成功与失败降级。
   - Pair 刷新时旧 Pair 正确 draining。
   - 通道复用场景下引用计数正确。

2. **集成测试**（client/pkg/pool_test.go / server/pkg/pool_test.go）：
   - 启用 Hot Pair 时代理请求首帧延迟低于退化路径。
   - `MsgChannelReset` 触发后客户端快速重建 Pair。

3. **并发测试**：
   - 多请求同时使用同一个 Pair，`refs` 计数正确。
   - 新旧 Pair 切换期间无数据丢失或重复。

## 10. 实现顺序

1. 扩展 `common/protocol.go`：新增 `MsgChannelReset` 与 `PrebindFlag`。
2. 服务端：
   - `ServerConnState` 增加 `isPrebind`。
   - `handleTCPConnect` 识别 Prebind。
   - `ServerWSConn` 增加 `hotPairRef`，`cleanupChannel` 支持延迟关闭。
   - 发送 `MsgChannelReset`。
3. 客户端：
   - 新增 `pair_warmer.go`。
   - 为通道增加引用计数。
   - `RegisterAndBroadcastTCP` 支持 Hot Pair 路径与退化路径。
   - `dialAndServe` 增加 fast retry 与动态节点切换。
4. 配置与命令行参数。
5. 单元测试与集成测试。

## 11. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 预绑定握手增加启动时资源消耗 | 限制 Pair 数量；可配置关闭 |
| 引用计数实现错误导致通道泄漏 | 单元测试覆盖；关闭前强制检查 ref |
| 快速重试过于激进增加服务端压力 | 限制窗口与次数；失败计数衰减 |
| 协议变更导致版本不兼容 | 新字段/消息均为可选；旧版本忽略 |

## 12. 验收标准

- [ ] 启用 `-hotpair` 后，SOCKS5 首帧响应时间比关闭时降低 30% 以上（在存在 TLS/WebSocket 握手延迟的环境下测试）。
- [ ] 节点瞬断（<5s）后，客户端在 1s 内恢复通道。
- [ ] 连续 1000 次短连接请求无通道泄漏或引用计数错误。
- [ ] 单元测试覆盖率不低于 70%。
