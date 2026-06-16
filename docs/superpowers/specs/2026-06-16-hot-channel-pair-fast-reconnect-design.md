# x-tunnel Hot Channel Pair 与快速重连设计

## 1. 背景与目标

x-tunnel 当前基于 WebSocket 多通道连接池，采用“请求到达后临时广播 `MsgTCPConnect`、服务端选上行、客户端选下行”的竞争机制。该机制在 SOCKS5/HTTP 代理短连接场景下会叠加以下延迟：

1. TLS + WebSocket 握手（若通道未就绪）。
2. 首次通道竞争的一次或多次 RTT。
3. 当前节点失败或通道断开时，固定 3s 基数指数退避重连。

本设计引入 **Hot Channel Pair（热通道对）** 与 **快速重连/节点切换**，目标：

- **降低首次响应延迟**：代理请求到达时复用已预热的最优上下行通道对。
- **加强链路稳定性**：动态重评链路质量并刷新 Hot Pair；失败时快速切换节点/通道。
- **保持架构一致**：复用现有通道选择逻辑，新增可选协议消息。

## 2. 核心概念

### 2.1 Hot Channel Pair

客户端维护若干已预先完成上下行绑定的通道对。每个 Pair 只保存通道 ID，不直接持有 `*websocket.Conn` 指针，以便与 `dialAndServe` 的重连逻辑保持一致。

```go
type HotChannelPair struct {
    ID           string
    UplinkChID   int
    DownlinkChID int
    ready        bool
    createdAt    time.Time
    refs         int32 // 当前正在使用该 Pair 的代理请求数
}
```

代理请求到达时，直接从当前主 Pair 中取出上下行通道 ID 发送首个消息，不再临时握手。发送时通过 `asyncWriteDirect(chID, ...)` 动态获取当前 `wsConns[idx]`。

### 2.2 预绑定（Prebind）

预绑定通过一次虚拟的 TCP 连接请求完成，沿用现有广播竞争逻辑。为避免破坏 `MsgTCPConnect` 的 meta 格式，使用独立消息类型 `MsgPrebindRequest`：

1. 客户端生成虚拟 `connID`（如 `prebind-<uuid>`）。
2. 客户端广播 `MsgPrebindRequest(connID, meta)`，meta 结构与 `MsgTCPConnect` 相同（`[0] IPStrategy; [1..n] target`），target 使用占位符 `x-tunnel.prebind`。
3. 服务端按现有逻辑选择上行通道并广播 `MsgSelectUplink`。
4. 客户端按现有逻辑选择下行通道并回传 `MsgSelectDownlink`。
5. 服务端识别 `MsgPrebindRequest` 后，不真正连接目标地址，仅完成通道选择、回复 `MsgConnStatus(StatusOK)`，并立即清理该预绑定 `ServerConnState`。

### 2.3 Pair 池与刷新

- 默认维护 `N=1` 个主 Hot Pair（可配置为更多）。
- 后台 **Pair Warmer** 周期性评估链路质量：
  - 与 `RelayNodeManager` 测速周期对齐（默认 30s~60s），或在测速完成后触发。
  - 若当前 Pair 评分仍最优，继续使用。
  - 若发现更优链路，启动新 Pair 构建。
- 新 Pair 就绪后成为“主 Pair”；旧 Pair 进入 `draining`：
  - 新请求只使用新 Pair。
  - 旧 Pair 的 `refs` 归零后标记为 closed，**不关闭其通道**（通道仍由 `dialAndServe` / `cleanupChannel` 管理）。
  - Pair 只记录通道 ID，不拥有通道。因此不存在“新旧 Pair 复用通道误关”问题，引用计数机制无需引入。

## 3. 协议变更

### 3.1 新增消息类型（common/protocol.go）

```go
const (
    MsgPrebindRequest MessageType = 0x10 // 预绑定请求
    MsgChannelReset   MessageType = 0x11 // 服务端通知客户端某通道需要重置
)
```

- `MsgPrebindRequest`：语义与 `MsgTCPConnect` 相同，但服务端不建立真实目标连接。
- `MsgChannelReset`：服务端在检测到某通道异常但尚未关闭 WebSocket 前，主动通知客户端该通道需要重置。客户端收到后立即废弃包含该通道的 Pair 并重建。

### 3.2 向后兼容

- 老版本服务端不认识 `MsgPrebindRequest`，会直接忽略（进入 `handleMessage` 的 `default` 分支或返回错误）。客户端检测到预绑定无响应后，退化到原有广播逻辑。
- 老版本客户端不认识 `MsgChannelReset`，直接忽略，不影响功能（依赖 WebSocket 断开重连）。

## 4. 服务端改动

### 4.1 ServerConnState 扩展

```go
type ServerConnState struct {
    // ... 现有字段
    isPrebind bool
}
```

### 4.2 handleMessage 增加 MsgPrebindRequest 分支

```go
func (p *serverPool) handleMessage(chID int, rawLen int, msgType common.MessageType, connID string, meta, payload []byte) {
    p.addReceivedBytes(rawLen)
    switch msgType {
    case common.MsgTCPConnect:
        p.handleTCPConnect(chID, connID, meta)

    case common.MsgPrebindRequest:
        p.handlePrebindRequest(chID, connID, meta)

    // ... 其他分支
    }
}
```

### 4.3 handlePrebindRequest

```go
func (p *serverPool) handlePrebindRequest(chID int, connID string, meta []byte) {
    ipStrategy := common.IPStrategy(meta[0])

    st := &ServerConnState{
        connID:     connID,
        uplinkChID: chID,
        ipStrategy: ipStrategy,
        isPrebind:  true,
        connected:  true,
    }

    p.mu.Lock()
    p.conns[connID] = st
    p.mu.Unlock()

    // 广播 MsgSelectUplink
    p.broadcastSelectUplinkPrebind(st)

    // 立即清理预绑定状态，避免泄漏
    p.unregisterConn(connID)
}
```

`broadcastSelectUplinkPrebind` 与现有 `broadcastSelectUplink` 类似，但 meta 中不携带真实目标信息，仅携带上行通道 ID。

### 4.4 MsgChannelReset 发送

在 `connection.go` 的 `writeLoop` 中，第一次检测到写失败但尚未调用 `wsConn.close()` 前，尝试发送 `MsgChannelReset`：

```go
if err := wsConn.writeDirect(websocket.PingMessage, []byte{}); err != nil {
    log.Printf("[服务端] 通道 %d ping发送失败: %v", wsConn.chID, err)
    _ = wsConn.notifyChannelReset()
    wsConn.close()
    return
}
```

如果 `notifyChannelReset` 自身也失败（连接已不可用），则忽略，依赖 WebSocket 断开重连。

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

- 监听通道就绪事件，在至少存在 `PairCount * 2` 个可用通道后启动预绑定。
- 构建 Pair 后标记 `ready`。
- 监听测速结果或定时器，触发 Pair 刷新。
- 管理 Pair 生命周期（ready / draining / closed）。

### 5.2 通道就绪通知

`clientPool` 增加通道就绪通知：

```go
type clientPool struct {
    // ... 现有字段
    chReadyCh chan int // 通道就绪通知（chID）
}
```

`dialAndServe` 成功建立连接后发送 `chReadyCh <- chID`。`PairWarmer` 监听该通道，在满足数量条件时触发预绑定。

### 5.3 代理请求复用 Hot Pair

`RegisterAndBroadcastTCP` 改为：

```go
func (p *clientPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
    pair := p.pairWarmer.AcquirePrimary()
    if pair == nil || !pair.ready {
        // 退化到原广播逻辑
        p.registerAndBroadcastFallback(connID, target, first, tcpConn, reqType)
        return
    }

    // 注册连接并关联到 Pair
    p.registerConnectionWithPair(connID, target, pair, tcpConn, reqType)

    // 直接通过主 Pair 的上行通道发送 MsgTCPConnect
    meta := make([]byte, 1+len(target))
    meta[0] = byte(p.config.IPStrategy)
    copy(meta[1:], target)
    msg := common.EncodeMessage(common.MsgTCPConnect, connID, meta, first)
    _ = p.asyncWriteDirect(pair.UplinkChID, websocket.BinaryMessage, msg)
}
```

### 5.4 快速重连与节点切换（方案 3）

在 `dialAndServe` 中：

- 写入/读取失败时，先进入 **fast retry** 模式：
  - 窗口默认 1s，带随机抖动（100~300ms）。
  - 默认最多 1 次快速重试。
  - 若 fast retry 成功，标记节点恢复。
- fast retry 失败后，再进入现有指数退避。
- 中转节点失败时，立即调用 `SelectNodeExcluding(lastIP)` 尝试新节点。
- 收到 `MsgChannelReset` 时，在 `handleChannel` 中解析 `chID`，通知 `PairWarmer` 废弃包含该通道的 Pair 并重建。

### 5.5 动态测速频率

`RelayNodeManager` 增加本地健康指数：

```go
type RelayNodeManager struct {
    // ...
    healthScore int32 // 0-100，基于最近失败率
}
```

- `healthScore < 30`：测速间隔缩短到 15s。
- `30 <= healthScore < 70`：默认 30s。
- `healthScore >= 70`：测速间隔延长到 60s。

## 6. 数据流

```
启动阶段:
  dialAndServe 建立通道 ──► chReadyCh ──► PairWarmer 等待足够通道
       │
       ▼
  广播 MsgPrebindRequest（复用现有通道选择竞争）
       │
       ▼
  服务端选择上行并广播 MsgSelectUplink
       │
       ▼
  客户端选择下行并回复 MsgSelectDownlink
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
                       旧 Pair refs=0 后标记 closed（不关闭通道）
```

## 7. 配置项

新增客户端配置：

```go
type Config struct {
    // ... 现有字段
    EnableHotPair          bool          // 是否启用热通道对
    HotPairCount           int           // Hot Pair 数量，默认 1
    HotPairRefreshInterval time.Duration // 默认 30s
    FastRetryAttempts      int           // 快速重试次数，默认 1
    FastRetryWindow        time.Duration // 快速重试窗口，默认 1s
}
```

命令行参数：

```bash
-hotpair              # 启用 Hot Pair
-hotpair-count 1      # Pair 数量
-hotpair-refresh 30s  # 刷新间隔
-fast-retry 1         # 快速重试次数
-fast-retry-window 1s # 快速重试窗口
```

## 8. 错误处理

- 预绑定失败：降级到原有广播逻辑，不影响功能。
- Hot Pair 不可用：退化到原有广播逻辑。
- Pair 释放时不关闭通道，通道生命周期仍由 `dialAndServe` 管理，避免误关。
- 服务端/客户端版本不一致：老版本忽略不认识的消息，行为与当前一致。

## 9. 测试策略

1. **单元测试**（client/pkg/pair_warmer_test.go）：
   - Pair 构建成功与失败降级。
   - Pair 刷新时旧 Pair 正确 draining。
   - 通道重连后 Pair 能正确检测并重建。

2. **集成测试**（client/pkg/pool_test.go / server/pkg/pool_test.go）：
   - 启用 Hot Pair 时代理请求首帧延迟低于退化路径。
   - `MsgChannelReset` 触发后客户端快速重建 Pair。
   - 老版本服务端兼容性：收到 `MsgPrebindRequest` 后退化正常。

3. **并发测试**：
   - 多请求同时使用同一个 Pair，`refs` 计数正确。
   - 新旧 Pair 切换期间无数据丢失或重复。

4. **服务端内存测试**：
   - 模拟 1000 次预绑定，验证 `p.conns` 大小不增长。

## 10. 实现顺序

1. 扩展 `common/protocol.go`：新增 `MsgPrebindRequest` 与 `MsgChannelReset`。
2. 服务端：
   - `handleMessage` 增加 `MsgPrebindRequest` 分支。
   - 新增 `handlePrebindRequest`，完成通道选择并立即清理预绑定状态。
   - 发送 `MsgChannelReset`。
3. 客户端：
   - 新增 `pair_warmer.go`。
   - 增加通道就绪通知机制。
   - `RegisterAndBroadcastTCP` 支持 Hot Pair 路径与退化路径。
   - `handleChannel` 处理 `MsgChannelReset`。
   - `dialAndServe` 增加 fast retry 与动态节点切换。
4. 配置与命令行参数。
5. 单元测试与集成测试。

## 11. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 预绑定握手增加启动时资源消耗 | 默认 1 个 Pair；可配置关闭 |
| Pair 持有旧 `wsConns[idx]` 指针导致向关闭连接写数据 | Pair 只保存 chID，发送时动态获取当前连接 |
| 预绑定状态泄漏 | 服务端立即 `unregisterConn` |
| 快速重试过于激进增加服务端压力 | 保守默认值、随机抖动、服务端状态感知 |
| 协议变更导致版本不兼容 | 新消息为可选；老版本忽略 |

## 12. 验收标准

- [ ] 启用 `-hotpair` 后，SOCKS5 首帧响应时间比关闭时降低 30% 以上（在存在 TLS/WebSocket 握手延迟的环境下测试）。
- [ ] 节点瞬断（<5s）后，客户端在 1s 内恢复通道。
- [ ] 连续 1000 次短连接请求无通道泄漏或 Pair 状态错误。
- [ ] 单元测试覆盖率不低于 70%。
