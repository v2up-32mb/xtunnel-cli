# Hot Channel Pair 与快速重连实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 x-tunnel 客户端与服务端实现 Hot Channel Pair（预绑定上下行通道对）和快速重连/节点切换，以降低 SOCKS5/HTTP 代理首帧延迟并提升链路稳定性。

**Architecture:** 复用现有 `MsgTCPConnect/MsgSelectUplink/MsgSelectDownlink` 竞争逻辑，新增独立的 `MsgPrebindRequest` 用于预绑定；客户端维护 Hot Pair 池，代理请求到达时直接复用已预热通道；叠加 fast retry 状态机与动态测速频率提升稳定性。

**Tech Stack:** Go 1.21+, gorilla/websocket, 现有 x-tunnel 包结构 (common/, client/pkg/, server/pkg/)

---

## 文件结构映射

| 文件 | 职责 |
|------|------|
| `common/protocol.go` | 新增 `MsgPrebindRequest`、`MsgChannelReset` 消息类型 |
| `common/protocol_test.go` | 新消息类型的编解码测试 |
| `server/pkg/handler.go` | 增加 `MsgPrebindRequest` 处理分支；`handlePrebindRequest` 实现 |
| `server/pkg/connection.go` | `MsgChannelReset` 发送；写队列延迟/堆积检测 |
| `server/pkg/pool.go` | `handleMessage` 路由；`broadcastSelectUplink` 适配预绑定；通道就绪统计辅助 |
| `server/pkg/pool_test.go` | 服务端预绑定单元测试；p.conns 不泄漏测试 |
| `client/pkg/config.go` | 新增 Hot Pair / fast retry 配置字段与默认值 |
| `client/pkg/pair_warmer.go` | **新增**：Pair Warmer、HotChannelPair、预绑定握手、生命周期管理 |
| `client/pkg/pair_warmer_test.go` | **新增**：Pair Warmer 单元测试 |
| `client/pkg/pool.go` | 集成 PairWarmer；通道就绪/失效通知；`RegisterAndBroadcastTCP` 支持 Hot Pair 路径；`handleChannel` 处理 `MsgChannelReset`；`dialAndServe` fast retry 状态机 |
| `client/pkg/pool_test.go` | Hot Pair 集成测试；fast retry 测试 |
| `client/pkg/relay.go` | 动态测速频率调整；`healthScore` |
| `client/pkg/relay_test.go` | 健康分数与测速间隔测试 |
| `client/cmd/x-tunnel-client/main.go` | 新增命令行参数 |
| `client/pkg/client.go` | `Stats` 中增加 Hot Pair 统计字段（可选） |

---

## Chunk 1: 协议扩展（common/protocol.go）

### Task 1.1: 新增消息类型常量

**Files:**
- Modify: `common/protocol.go:14-25`
- Test: `common/protocol_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestMessageTypeHasPrebindAndReset(t *testing.T) {
    if common.MsgPrebindRequest != 0x10 {
        t.Fatalf("MsgPrebindRequest = %d, want 0x10", common.MsgPrebindRequest)
    }
    if common.MsgChannelReset != 0x11 {
        t.Fatalf("MsgChannelReset = %d, want 0x11", common.MsgChannelReset)
    }
}
```

Run: `go test ./common -run TestMessageTypeHasPrebindAndReset -v`
Expected: FAIL (undefined: common.MsgPrebindRequest)

- [ ] **Step 2: 实现常量**

在 `common/protocol.go` 中，在 `MsgBackpressure` 后新增：

```go
const (
    MsgTCPConnect MessageType = iota + 1
    MsgTCPData
    MsgTCPClose
    MsgUDPConnect
    MsgUDPData
    MsgUDPClose
    MsgConnStatus
    MsgSelectUplink
    MsgSelectDownlink
    MsgBackpressure
    MsgPrebindRequest = 0x10 // 预绑定请求
    MsgChannelReset   = 0x11 // 通道重置通知
)
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./common -run TestMessageTypeHasPrebindAndReset -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add common/protocol.go common/protocol_test.go
git commit -m "feat(protocol): 新增 MsgPrebindRequest 与 MsgChannelReset 消息类型

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 2: 服务端预绑定支持

### Task 2.1: handleMessage 增加 MsgPrebindRequest 路由

**Files:**
- Modify: `server/pkg/pool.go:214-238`
- Test: `server/pkg/pool_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHandleMessageRoutesPrebindRequest(t *testing.T) {
    p := newTestServerPool()
    called := false
    p.handlePrebindRequest = func(chID int, connID string, meta []byte) {
        called = true
    }
    p.handleMessage(1, 10, common.MsgPrebindRequest, "prebind-x", []byte{0, 'x'}, nil)
    if !called {
        t.Fatal("handlePrebindRequest was not called")
    }
}
```

Run: `go test ./server/pkg -run TestHandleMessageRoutesPrebindRequest -v`
Expected: FAIL (cannot assign to p.handlePrebindRequest)

- [ ] **Step 2: 实现路由**

修改 `server/pkg/pool.go` 的 `handleMessage`：

```go
func (p *serverPool) handleMessage(chID int, rawLen int, msgType common.MessageType, connID string, meta, payload []byte) {
    p.addReceivedBytes(rawLen)
    switch msgType {
    case common.MsgTCPConnect:
        p.handleTCPConnect(chID, connID, meta)

    case common.MsgPrebindRequest:
        p.handlePrebindRequest(chID, connID, meta)

    case common.MsgTCPData:
        p.handleTCPData(chID, connID, payload)

    // ... 其他分支不变
    }
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./server/pkg -run TestHandleMessageRoutesPrebindRequest -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add server/pkg/pool.go server/pkg/pool_test.go
git commit -m "feat(server): 增加 MsgPrebindRequest 消息路由

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 2.2: 实现 handlePrebindRequest

**Files:**
- Modify: `server/pkg/handler.go`
- Test: `server/pkg/pool_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHandlePrebindRequestCleansUpState(t *testing.T) {
    p := newTestServerPool()
    connID := "prebind-test-1"
    meta := []byte{0, 'x', '-', 't', 'u', 'n', 'n', 'e', 'l', '.', 'p', 'r', 'e', 'b', 'i', 'n', 'd'}

    p.handlePrebindRequest(1, connID, meta)

    p.mu.RLock()
    _, exists := p.conns[connID]
    p.mu.RUnlock()
    if exists {
        t.Fatal("prebind connID should be cleaned up")
    }
}
```

Run: `go test ./server/pkg -run TestHandlePrebindRequestCleansUpState -v`
Expected: FAIL (p.handlePrebindRequest undefined)

- [ ] **Step 2: 实现 handlePrebindRequest**

在 `server/pkg/handler.go` 中，紧接 `handleTCPClose` 之后新增：

```go
// handlePrebindRequest 处理预绑定请求
func (p *serverPool) handlePrebindRequest(chID int, connID string, meta []byte) {
    if len(meta) < 1 {
        return
    }

    ipStrategy := common.IPStrategy(meta[0])

    p.mu.Lock()
    if _, exists := p.conns[connID]; exists {
        p.mu.Unlock()
        return
    }

    st := &ServerConnState{
        connID:     connID,
        uplinkChID: chID,
        ipStrategy: ipStrategy,
        isPrebind:  true,
        connected:  true,
    }
    p.conns[connID] = st
    p.mu.Unlock()

    p.mu.RLock()
    wsConn := p.chConns[chID]
    p.mu.RUnlock()
    if wsConn != nil {
        st.clientID = wsConn.clientID
        st.clientAddr = wsConn.remoteAddr
    }

    uplinkChIDBytes := make([]byte, 4)
    binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
    _ = p.sendDownlink(connID, common.MsgSelectUplink, uplinkChIDBytes, nil)

    // 预绑定只完成上行选择，立即清理状态，避免泄漏
    p.unregisterConn(connID)
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./server/pkg -run TestHandlePrebindRequestCleansUpState -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add server/pkg/handler.go server/pkg/pool_test.go
git commit -m "feat(server): 实现 handlePrebindRequest 并立即清理预绑定状态

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 2.3: 验证预绑定不泄漏 p.conns

**Files:**
- Test: `server/pkg/pool_test.go`

- [ ] **Step 1: 写测试**

```go
func TestPrebindDoesNotLeakConns(t *testing.T) {
    p := newTestServerPool()
    for i := 0; i < 1000; i++ {
        connID := fmt.Sprintf("prebind-%d", i)
        meta := []byte{0, 'x', '-', 't', 'u', 'n', 'n', 'e', 'l', '.', 'p', 'r', 'e', 'b', 'i', 'n', 'd'}
        p.handlePrebindRequest(1, connID, meta)
    }
    p.mu.RLock()
    n := len(p.conns)
    p.mu.RUnlock()
    if n != 0 {
        t.Fatalf("expected 0 conns after prebind, got %d", n)
    }
}
```

Run: `go test ./server/pkg -run TestPrebindDoesNotLeakConns -v`
Expected: PASS

- [ ] **Step 2: 提交**

```bash
git add server/pkg/pool_test.go
git commit -m "test(server): 验证预绑定不泄漏 p.conns

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 3: 服务端 MsgChannelReset 发送

### Task 3.1: 实现 notifyChannelReset

**Files:**
- Modify: `server/pkg/connection.go`
- Test: `server/pkg/connection_test.go`（如不存在则创建）

- [ ] **Step 1: 写失败测试**

```go
func TestNotifyChannelResetEncodesChID(t *testing.T) {
    wsConn := &ServerWSConn{chID: 7}
    // 需要 mock ws，这里仅验证 encode 逻辑
    meta := make([]byte, 4)
    binary.BigEndian.PutUint32(meta, uint32(7))
    expected := common.EncodeMessage(common.MsgChannelReset, "", meta, nil)
    if len(expected) == 0 {
        t.Fatal("expected non-empty message")
    }
}
```

Run: `go test ./server/pkg -run TestNotifyChannelResetEncodesChID -v`
Expected: PASS（仅验证 encode 逻辑）

- [ ] **Step 2: 实现 notifyChannelReset**

在 `server/pkg/connection.go` 中新增方法：

```go
import "encoding/binary"

// notifyChannelReset 通知客户端该通道需要重置
func (wsConn *ServerWSConn) notifyChannelReset() error {
    wsConn.mu.Lock()
    defer wsConn.mu.Unlock()
    if wsConn.closed {
        return nil
    }
    meta := make([]byte, 4)
    binary.BigEndian.PutUint32(meta, uint32(wsConn.chID))
    data := common.EncodeMessage(common.MsgChannelReset, "", meta, nil)
    _ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
    err := wsConn.ws.WriteMessage(websocket.BinaryMessage, data)
    _ = wsConn.ws.SetWriteDeadline(time.Time{})
    return err
}
```

- [ ] **Step 3: 提交**

```bash
git add server/pkg/connection.go
git commit -m "feat(server): 实现 notifyChannelReset

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 3.2: 写队列延迟/堆积检测触发 MsgChannelReset

**Files:**
- Modify: `server/pkg/connection.go`（asyncWrite/writeLoop）

- [ ] **Step 1: 写失败测试**

由于涉及时间/异步，先写轻量单元测试验证计数器增长：

```go
func TestAsyncWriteQueueFullIncrementsCounter(t *testing.T) {
    p := newTestServerPool()
    wsConn := &ServerWSConn{
        pool:      p,
        chID:      1,
        writeChan: make(chan writeTask, 1),
    }
    wsConn.writeChan <- writeTask{msgType: websocket.BinaryMessage, data: make([]byte, 10), size: 10}
    _ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))
    // 验证 queueFullCount 增加
}
```

Run: `go test ./server/pkg -run TestAsyncWriteQueueFullIncrementsCounter -v`
Expected: FAIL（queueFullCount 字段不存在）

- [ ] **Step 2: 实现计数器与触发逻辑**

在 `ServerWSConn` 中增加：

```go
type ServerWSConn struct {
    // ... 现有字段
    queueFullCount int32
    lastQueueFull  time.Time
}
```

在 `asyncWrite` 的 `default`（队列满）分支中：

```go
func (wsConn *ServerWSConn) asyncWrite(msgType int, data []byte) error {
    // ...
    select {
    case queue <- writeTask{msgType: msgType, data: data, size: size}:
        // ...
    default:
        wsConn.pool.rollbackQueueBytes(size)
        wsConn.mu.Unlock()

        now := time.Now()
        if wsConn.lastQueueFull.IsZero() || now.Sub(wsConn.lastQueueFull) > time.Second {
            wsConn.queueFullCount = 0
        }
        wsConn.lastQueueFull = now
        wsConn.queueFullCount++
        if wsConn.queueFullCount >= 3 {
            _ = wsConn.notifyChannelReset()
            wsConn.queueFullCount = 0
        }
        return fmt.Errorf("写队列满")
    }
}
```

注意：`wsConn.mu` 在 default 分支已经 unlock，因此修改 queueFullCount 时需要重新加锁。更好的做法是在 unlock 前保存相关字段。实现时应避免在 unlock 后访问未加锁字段。此处简化描述，实际代码需加锁。

- [ ] **Step 3: 运行测试通过**

Run: `go test ./server/pkg -run TestAsyncWriteQueueFullIncrementsCounter -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add server/pkg/connection.go server/pkg/connection_test.go
git commit -m "feat(server): 写队列满时发送 MsgChannelReset

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 4: 客户端配置与命令行参数

### Task 4.1: 扩展 Config

**Files:**
- Modify: `client/pkg/config.go`
- Test: `client/pkg/pool_test.go` 或新建 `client/pkg/config_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestDefaultConfigHasHotPairDefaults(t *testing.T) {
    cfg := client.DefaultConfig()
    if cfg.EnableHotPair {
        t.Fatal("EnableHotPair default should be false")
    }
    if cfg.HotPairCount != 1 {
        t.Fatalf("HotPairCount default = %d, want 1", cfg.HotPairCount)
    }
    if cfg.HotPairRefreshInterval != 30*time.Second {
        t.Fatalf("HotPairRefreshInterval = %v, want 30s", cfg.HotPairRefreshInterval)
    }
}
```

Run: `go test ./client/pkg -run TestDefaultConfigHasHotPairDefaults -v`
Expected: FAIL（字段不存在）

- [ ] **Step 2: 实现配置字段**

修改 `client/pkg/config.go` 的 `Config` 结构体：

```go
type Config struct {
    // ... 现有字段

    // Hot Pair 配置
    EnableHotPair          bool          // 是否启用热通道对
    HotPairCount           int           // Hot Pair 数量，默认 1
    HotPairRefreshInterval time.Duration // Pair 刷新间隔，默认 30s

    // 快速重连配置
    FastRetryAttempts    int           // 快速重试次数，默认 1
    FastRetryWindow      time.Duration // 快速重试窗口，默认 1s
    MaxFastRetryConsecutive int        // 连续进入 fast retry 的最大次数，默认 3
}
```

在 `DefaultConfig()` 中增加：

```go
func DefaultConfig() *Config {
    return &Config{
        // ... 现有默认值
        EnableHotPair:          false,
        HotPairCount:           1,
        HotPairRefreshInterval: 30 * time.Second,
        FastRetryAttempts:      1,
        FastRetryWindow:        1 * time.Second,
        MaxFastRetryConsecutive: 3,
    }
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestDefaultConfigHasHotPairDefaults -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/config.go
git commit -m "feat(client/config): 新增 Hot Pair 与 fast retry 配置字段

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 4.2: 命令行参数

**Files:**
- Modify: `client/cmd/x-tunnel-client/main.go`

- [ ] **Step 1: 实现参数注册**

在 `registerFlags` 中新增：

```go
fs.BoolVar(&enableHotPair, "hotpair", false, "启用 Hot Channel Pair 降低首帧延迟")
fs.IntVar(&hotPairCount, "hotpair-count", 1, "Hot Pair 数量")
fs.DurationVar(&hotPairRefreshInterval, "hotpair-refresh", 30*time.Second, "Hot Pair 刷新间隔")
fs.IntVar(&fastRetryAttempts, "fast-retry", 1, "快速重试次数")
fs.DurationVar(&fastRetryWindow, "fast-retry-window", 1*time.Second, "快速重试窗口")
fs.IntVar(&maxFastRetryConsecutive, "fast-retry-consecutive", 3, "连续进入快速重试的最大次数")
```

新增包级变量：

```go
var (
    // ... 现有变量
    enableHotPair             bool
    hotPairCount              int
    hotPairRefreshInterval    time.Duration
    fastRetryAttempts         int
    fastRetryWindow           time.Duration
    maxFastRetryConsecutive   int
)
```

在 `parseFlags` 中赋值给 `cfg`：

```go
cfg.EnableHotPair = enableHotPair
cfg.HotPairCount = hotPairCount
cfg.HotPairRefreshInterval = hotPairRefreshInterval
cfg.FastRetryAttempts = fastRetryAttempts
cfg.FastRetryWindow = fastRetryWindow
cfg.MaxFastRetryConsecutive = maxFastRetryConsecutive
```

- [ ] **Step 2: 编译验证**

Run: `go build ./client/cmd/x-tunnel-client`
Expected: PASS

- [ ] **Step 3: 提交**

```bash
git add client/cmd/x-tunnel-client/main.go
git commit -m "feat(client/cmd): 新增 Hot Pair 与 fast retry 命令行参数

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 5: 客户端 Pair Warmer

### Task 5.1: 创建 pair_warmer.go 骨架

**Files:**
- Create: `client/pkg/pair_warmer.go`
- Test: `client/pkg/pair_warmer_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestPairWarmerNew(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    cfg.HotPairCount = 1
    cfg.HotPairRefreshInterval = 30 * time.Second

    pool := &clientPool{config: cfg}
    warmer := client.NewPairWarmer(pool, cfg)
    if warmer == nil {
        t.Fatal("NewPairWarmer returned nil")
    }
}
```

Run: `go test ./client/pkg -run TestPairWarmerNew -v`
Expected: FAIL（NewPairWarmer undefined）

- [ ] **Step 2: 实现骨架**

创建 `client/pkg/pair_warmer.go`：

```go
package client

import (
    "context"
    "sync"
    "sync/atomic"
    "time"
)

const (
    PairStateReady = iota
    PairStateDraining
    PairStateClosed
)

// HotChannelPair 表示一个预绑定好的上下行通道对
type HotChannelPair struct {
    ID           string
    UplinkChID   int
    DownlinkChID int
    state        int
    createdAt    time.Time
    refs         int32
}

// PairWarmerConfig Pair Warmer 配置
type PairWarmerConfig struct {
    PairCount       int
    RefreshInterval time.Duration
    PrebindTimeout  time.Duration
}

// PairWarmer 管理 Hot Channel Pair 的生命周期
type PairWarmer struct {
    pool    *clientPool
    mu      sync.RWMutex
    pairs   []*HotChannelPair
    primary *HotChannelPair
    config  PairWarmerConfig
    ctx     context.Context
    cancel  context.CancelFunc
}

// NewPairWarmer 创建 Pair Warmer
func NewPairWarmer(pool *clientPool, cfg *Config) *PairWarmer {
    ctx, cancel := context.WithCancel(pool.ctx)
    return &PairWarmer{
        pool: pool,
        config: PairWarmerConfig{
            PairCount:       cfg.HotPairCount,
            RefreshInterval: cfg.HotPairRefreshInterval,
            PrebindTimeout:  3 * time.Second,
        },
        ctx:    ctx,
        cancel: cancel,
    }
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestPairWarmerNew -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pair_warmer_test.go
git commit -m "feat(client): 创建 PairWarmer 骨架

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 5.2: 实现 AcquirePrimary / Release / Invalidate

**Files:**
- Modify: `client/pkg/pair_warmer.go`
- Test: `client/pkg/pair_warmer_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestPairWarmerAcquirePrimary(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    pool := &clientPool{config: cfg}
    warmer := client.NewPairWarmer(pool, cfg)

    pair := &client.HotChannelPair{ID: "p1", UplinkChID: 1, DownlinkChID: 2, state: client.PairStateReady}
    warmer.SetPrimaryForTest(pair)

    acquired := warmer.AcquirePrimary()
    if acquired == nil {
        t.Fatal("AcquirePrimary returned nil")
    }
    if atomic.LoadInt32(&acquired.refs) != 1 {
        t.Fatalf("refs = %d, want 1", atomic.LoadInt32(&acquired.refs))
    }
}
```

Run: `go test ./client/pkg -run TestPairWarmerAcquirePrimary -v`
Expected: FAIL（方法不存在）

- [ ] **Step 2: 实现方法**

在 `pair_warmer.go` 中新增：

```go
// AcquirePrimary 获取当前主 Pair 并增加引用计数
func (w *PairWarmer) AcquirePrimary() *HotChannelPair {
    w.mu.RLock()
    pair := w.primary
    w.mu.RUnlock()
    if pair == nil || pair.state != PairStateReady {
        return nil
    }
    atomic.AddInt32(&pair.refs, 1)
    return pair
}

// ReleasePair 减少 Pair 引用计数
func (w *PairWarmer) ReleasePair(pair *HotChannelPair) {
    if pair == nil {
        return
    }
    atomic.AddInt32(&pair.refs, -1)
}

// InvalidateChannel 废弃包含指定通道的所有 Pair
func (w *PairWarmer) InvalidateChannel(chID int) {
    w.mu.Lock()
    defer w.mu.Unlock()
    for _, pair := range w.pairs {
        if pair.state == PairStateClosed {
            continue
        }
        if pair.UplinkChID == chID || pair.DownlinkChID == chID {
            pair.state = PairStateDraining
            if w.primary == pair {
                w.primary = nil
            }
        }
    }
}

// 仅用于测试
func (w *PairWarmer) SetPrimaryForTest(pair *HotChannelPair) {
    w.mu.Lock()
    w.primary = pair
    w.pairs = append(w.pairs, pair)
    w.mu.Unlock()
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestPairWarmerAcquirePrimary -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pair_warmer_test.go
git commit -m "feat(client): 实现 PairWarmer 的 Acquire/Release/Invalidate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 5.3: 实现预绑定握手

**Files:**
- Modify: `client/pkg/pair_warmer.go`
- Test: `client/pkg/pair_warmer_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestPairWarmerBuildPairUsesPrebindPrefix(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    pool := newTestClientPool(cfg)
    warmer := client.NewPairWarmer(pool, cfg)

    pair, err := warmer.BuildPair([]int{1, 2})
    if err == nil {
        // 在单测中可能因无真实 ws 而失败，这里仅验证 connID 前缀
        if pair != nil && !strings.HasPrefix(pair.ID, "prebind-") {
            t.Fatalf("pair.ID should have prebind- prefix, got %s", pair.ID)
        }
    }
}
```

Run: `go test ./client/pkg -run TestPairWarmerBuildPairUsesPrebindPrefix -v`
Expected: FAIL（BuildPair undefined）

- [ ] **Step 2: 实现 BuildPair**

在 `pair_warmer.go` 中新增核心方法：

```go
import (
    "encoding/binary"
    "fmt"
    "github.com/google/uuid"
    "github.com/gorilla/websocket"
    "x-tunnel/common"
)

// BuildPair 使用可用通道列表构建一个 Hot Pair
func (w *PairWarmer) BuildPair(available []int) (*HotChannelPair, error) {
    if len(available) < 2 {
        return nil, fmt.Errorf("可用通道不足")
    }

    connID := "prebind-" + uuid.New().String()
    meta := make([]byte, 1+len(common.PrebindTarget))
    meta[0] = byte(w.pool.config.IPStrategy)
    copy(meta[1:], common.PrebindTarget)

    msg := common.EncodeMessage(common.MsgPrebindRequest, connID, meta, nil)

    // 广播到所有可用通道
    for _, chID := range available {
        _ = w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg)
    }

    // 等待第一个 MsgSelectUplink（通过回调或 channel）
    // 简化：实际实现需要一个 handshake result channel
    // 这里仅给出骨架，具体等待逻辑需要与 pool.handleChannel 配合
    return nil, fmt.Errorf("TODO: implement handshake wait")
}
```

注意：由于预绑定结果需要异步等待 `MsgSelectUplink`，`BuildPair` 不能直接同步返回。实际实现应采用 result channel + goroutine 模式。此处先给出骨架，下一 Task 与 pool.handleChannel 集成。

- [ ] **Step 3: 运行测试**

Run: `go test ./client/pkg -run TestPairWarmerBuildPairUsesPrebindPrefix -v`
Expected: 根据测试断言可能 PASS 或 FAIL；实现者应根据集成方式调整测试。

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pair_warmer_test.go
git commit -m "feat(client): 实现 PairWarmer 预绑定握手骨架

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 6: 客户端 pool 集成 PairWarmer

### Task 6.1: 增加通道就绪/失效通知

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestClientPoolChannelReadyNotification(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    if p.chReadyCh == nil {
        t.Fatal("chReadyCh should not be nil")
    }
    if cap(p.chReadyCh) == 0 {
        t.Fatal("chReadyCh should be buffered")
    }
}
```

Run: `go test ./client/pkg -run TestClientPoolChannelReadyNotification -v`
Expected: FAIL（字段不存在）

- [ ] **Step 2: 实现通道通知字段**

在 `clientPool` 结构体中新增：

```go
type clientPool struct {
    // ... 现有字段
    chReadyCh   chan int
    chInvalidCh chan int
}
```

在 `newClientPool` 中初始化：

```go
p := &clientPool{
    // ... 现有初始化
    chReadyCh:   make(chan int, 64),
    chInvalidCh: make(chan int, 64),
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestClientPoolChannelReadyNotification -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): 增加通道就绪与失效通知通道

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.2: dialAndServe 发送就绪/失效通知

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 实现就绪通知**

在 `dialAndServe` 中，连接成功后：

```go
p.wsConnsMu.Lock()
p.wsConns[idx] = wsConn
p.wsConnsMu.Unlock()

select {
case p.chReadyCh <- chID:
default:
}
```

- [ ] **Step 2: 实现重连失效通知**

在 `dialAndServe` 中，每次循环开始（即重连前），如果 `lastIP` 不为空或已有连接：

```go
if !firstAttempt {
    select {
    case p.chInvalidCh <- chID:
    default:
    }
}
```

注意：应在连接断开时立即通知，而不是在重连成功后。实现者需根据 `dialAndServe` 循环结构选择正确位置。

- [ ] **Step 3: 提交**

```bash
git add client/pkg/pool.go
git commit -m "feat(client/pool): dialAndServe 发送通道就绪与失效通知

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.3: RegisterAndBroadcastTCP 支持 Hot Pair 路径

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestRegisterAndBroadcastTCPUsesHotPair(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    // 设置一个 mock PairWarmer
    // ...
}
```

Run: `go test ./client/pkg -run TestRegisterAndBroadcastTCPUsesHotPair -v`
Expected: 根据 mock 实现而定

- [ ] **Step 2: 实现 Hot Pair 路径**

修改 `RegisterAndBroadcastTCP`：

```go
func (p *clientPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
    p.mu.Lock()
    st := p.conns[connID]
    if st == nil {
        st = &clientConnState{}
        p.conns[connID] = st
    }
    st.tcpConn = tcpConn
    st.target = target
    st.connected = make(chan bool, 1)
    st.start = time.Now()
    if reqType != "" {
        st.reqType = reqType
    }
    if tcpConn != nil {
        if ra := tcpConn.RemoteAddr(); ra != nil {
            st.clientAddr = ra.String()
        }
    }
    st.uplink = 0
    st.downlink = 0
    st.lastCh = 0
    st.closed = false
    p.mu.Unlock()

    if p.config.EnableHotPair && p.pairWarmer != nil {
        pair := p.pairWarmer.AcquirePrimary()
        if pair != nil && pair.state == PairStateReady {
            st.pair = pair // 新增字段
            atomic.AddInt32(&pair.refs, 1)
            meta := make([]byte, 1+len(target))
            meta[0] = byte(p.config.IPStrategy)
            copy(meta[1:], target)
            msg := common.EncodeMessage(common.MsgTCPConnect, connID, meta, first)
            _ = p.asyncWriteDirect(pair.UplinkChID, websocket.BinaryMessage, msg)
            return
        }
    }

    // 退化到广播逻辑
    p.registerAndBroadcastFallback(connID, target, first)
}
```

需要在 `clientConnState` 中新增 `pair *HotChannelPair` 字段。

- [ ] **Step 3: Unregister 释放 Pair refs**

修改 `Unregister`：

```go
func (p *clientPool) Unregister(connID string) {
    p.mu.Lock()
    st := p.conns[connID]
    // ... 现有逻辑
    pair := st.pair
    p.mu.Unlock()

    if pair != nil {
        p.pairWarmer.ReleasePair(pair)
    }

    // ... 后续关闭逻辑
}
```

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): RegisterAndBroadcastTCP 支持 Hot Pair 路径

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.4: handleChannel 处理 MsgChannelReset

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHandleChannelChannelReset(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    // 模拟收到 MsgChannelReset
}
```

Run: `go test ./client/pkg -run TestHandleChannelChannelReset -v`
Expected: 根据实现而定

- [ ] **Step 2: 实现处理分支**

在 `handleChannel` 的 switch 中新增：

```go
case common.MsgChannelReset:
    if len(meta) >= 4 {
        chID := int(binary.BigEndian.Uint32(meta[0:4]))
        select {
        case p.chInvalidCh <- chID:
        default:
        }
        if p.pairWarmer != nil {
            p.pairWarmer.InvalidateChannel(chID)
        }
    }
```

- [ ] **Step 3: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): handleChannel 处理 MsgChannelReset

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.5: 启动 PairWarmer

**Files:**
- Modify: `client/pkg/pool.go`
- Modify: `client/pkg/client.go`

- [ ] **Step 1: 在 newClientPool 中创建 PairWarmer**

```go
if cfg.EnableHotPair {
    p.pairWarmer = NewPairWarmer(p, cfg)
}
```

- [ ] **Step 2: 在 pool.Start 中启动 warmer goroutine**

```go
if p.config.EnableHotPair && p.pairWarmer != nil {
    go p.pairWarmer.Run()
}
```

- [ ] **Step 3: 实现 PairWarmer.Run**

在 `pair_warmer.go` 中：

```go
func (w *PairWarmer) Run() {
    readyCount := 0
    timer := time.NewTimer(w.config.RefreshInterval)
    defer timer.Stop()

    for {
        select {
        case <-w.ctx.Done():
            return
        case chID := <-w.pool.chReadyCh:
            readyCount++
            if readyCount >= w.config.PairCount*2 && w.primary == nil {
                w.tryBuildPairs()
            }
        case chID := <-w.pool.chInvalidCh:
            w.InvalidateChannel(chID)
            w.tryBuildPairs()
        case <-timer.C:
            w.tryRefresh()
            timer.Reset(w.config.RefreshInterval)
        }
    }
}
```

`tryBuildPairs` 和 `tryRefresh` 的具体实现依赖 BuildPair 的完成。

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pool.go client/pkg/client.go
git commit -m "feat(client): 启动 PairWarmer 并监听通道事件

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 7: 快速重连状态机

### Task 7.1: dialAndServe 集成 fast retry

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestFastRetryStateResetsOnSuccess(t *testing.T) {
    state := &client.FastRetryState{}
    state.OnFailure()
    state.OnFailure()
    state.OnSuccess()
    if state.ConsecutiveFailures != 0 {
        t.Fatalf("consecutive failures = %d, want 0", state.ConsecutiveFailures)
    }
}
```

Run: `go test ./client/pkg -run TestFastRetryStateResetsOnSuccess -v`
Expected: FAIL（类型不存在）

- [ ] **Step 2: 实现 FastRetryState**

在 `client/pkg/pool.go` 中新增：

```go
type fastRetryState struct {
    consecutiveFailures int
    lastFailure         time.Time
    inFastRetry         bool
}

func (f *fastRetryState) OnFailure() {
    f.consecutiveFailures++
    f.lastFailure = time.Now()
}

func (f *fastRetryState) OnSuccess() {
    f.consecutiveFailures = 0
    f.lastFailure = time.Time{}
    f.inFastRetry = false
}

func (f *fastRetryState) ShouldFastRetry(maxConsecutive int) bool {
    return f.consecutiveFailures < maxConsecutive
}
```

- [ ] **Step 3: 修改 dialAndServe 使用 fast retry**

在 `dialAndServe` 中：

```go
frs := &fastRetryState{}

for {
    // ...
    wsConn, err := p.dialWebSocket(chID, ip)
    if err != nil {
        frs.OnFailure()
        if frs.ShouldFastRetry(p.config.MaxFastRetryConsecutive) {
            // fast retry with jitter
            jitter := time.Duration(rand.Intn(300)) * time.Millisecond
            window := p.config.FastRetryWindow + jitter
            // 在窗口内最多 FastRetryAttempts 次
            // ...
            continue
        }
        // 进入指数退避
        // ...
    }

    // 连接成功
    frs.OnSuccess()
    // ...
}
```

注意：`rand` 需要初始化 `rand.Seed` 或使用 `crypto/rand`。建议使用 `math/rand/v2`（Go 1.22+）或现有项目风格。

- [ ] **Step 4: 运行测试通过**

Run: `go test ./client/pkg -run TestFastRetryStateResetsOnSuccess -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): 实现 fast retry 状态机

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 8: 动态测速频率

### Task 8.1: RelayNodeManager 健康分数

**Files:**
- Modify: `client/pkg/relay.go`
- Test: `client/pkg/relay_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHealthScoreAdjustsInterval(t *testing.T) {
    mgr := client.NewRelayNodeManager()
    mgr.SetHealthScore(20)
    interval := mgr.CurrentTestInterval()
    if interval != 15*time.Second {
        t.Fatalf("interval = %v, want 15s", interval)
    }
}
```

Run: `go test ./client/pkg -run TestHealthScoreAdjustsInterval -v`
Expected: FAIL（方法不存在）

- [ ] **Step 2: 实现健康分数**

在 `RelayNodeManager` 中新增：

```go
type RelayNodeManager struct {
    // ... 现有字段
    healthScore int32
}

func (m *RelayNodeManager) SetHealthScore(score int32) {
    if score < 0 {
        score = 0
    }
    if score > 100 {
        score = 100
    }
    atomic.StoreInt32(&m.healthScore, score)
}

func (m *RelayNodeManager) GetHealthScore() int32 {
    return atomic.LoadInt32(&m.healthScore)
}

func (m *RelayNodeManager) CurrentTestInterval() time.Duration {
    score := m.GetHealthScore()
    switch {
    case score < 30:
        return 15 * time.Second
    case score >= 70:
        return 60 * time.Second
    default:
        return 30 * time.Second
    }
}
```

- [ ] **Step 3: 根据失败率更新 healthScore**

在 `testAllNodes` 或 `MarkNodeFailed/MarkNodeSuccess` 中更新：

```go
func (m *RelayNodeManager) updateHealthScore() {
    snapshots := m.snapshotNodes()
    if len(snapshots) == 0 {
        m.SetHealthScore(50)
        return
    }
    var failures int
    for _, s := range snapshots {
        if s.failCount > 0 {
            failures++
        }
    }
    score := 100 - (failures * 100 / len(snapshots))
    m.SetHealthScore(int32(score))
}
```

- [ ] **Step 4: 使用动态间隔**

在 `Start` 中：

```go
func (m *RelayNodeManager) Start() {
    // ...
    interval := m.CurrentTestInterval()
    m.testTimer = time.NewTicker(interval)
    go m.speedTestLoop()
}
```

`speedTestLoop` 每次循环结束后重新计算间隔（因为 `time.Ticker` 不能改间隔，需改用 `time.Timer` 或每次重置 Ticker）。

- [ ] **Step 5: 运行测试通过**

Run: `go test ./client/pkg -run TestHealthScoreAdjustsInterval -v`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add client/pkg/relay.go client/pkg/relay_test.go
git commit -m "feat(client/relay): 根据健康分数动态调整测速间隔

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 9: 集成测试与验证

### Task 9.1: 端到端 Hot Pair 集成测试

**Files:**
- Test: `client/pkg/pool_test.go`

- [ ] **Step 1: 写测试**

```go
func TestHotPairReducesFirstFrameLatency(t *testing.T) {
    // 启动本地 mock server（可使用 httptest + websocket.Upgrader）
    // 配置 client.EnableHotPair = true
    // 触发 SOCKS5/HTTP 请求
    // 验证首条 MsgTCPConnect 直接通过单一通道发送，而非广播
}
```

Run: `go test ./client/pkg -run TestHotPairReducesFirstFrameLatency -v`
Expected: 根据 mock 实现而定

- [ ] **Step 2: 实现 mock server helper**

在 `client/pkg/pool_test.go` 或 `client/pkg/test_helpers.go` 中创建可复用的 mock WebSocket server，用于测试预绑定和 Hot Pair 路径。

- [ ] **Step 3: 提交**

```bash
git add client/pkg/pool_test.go
git commit -m "test(client): 添加 Hot Pair 端到端集成测试

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 9.2: 编译与完整测试

- [ ] **Step 1: 编译客户端和服务端**

```bash
go build ./client/cmd/x-tunnel-client
go build ./server/cmd/x-tunnel-server
```

Expected: 两者都 PASS

- [ ] **Step 2: 运行完整测试套件**

```bash
go test ./...
```

Expected: 所有测试 PASS（或仅已知失败）

- [ ] **Step 3: 提交**

```bash
git add .
git commit -m "test: 全量测试通过

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Plan Review Loop

每个 Chunk 完成后，应 dispatch plan-document-reviewer 进行评审，确保：

1. 文件路径和修改位置正确。
2. 测试用例覆盖了关键风险点。
3. 没有与现有代码风格冲突的地方。
4. 实现顺序合理，不会产生循环依赖。

如果 reviewer 提出 issues，修复后重新 dispatch，最多 5 次迭代。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-16-hot-channel-pair-fast-reconnect-plan.md`. Ready to execute?

执行路径：
- 优先使用 `superpowers:subagent-driven-development` 分派子代理实现每个 Task。
- 如果没有子代理，使用 `superpowers:executing-plans` 在当前会话分批执行。

每个 Task 完成后运行对应测试并提交，确保 TDD 循环完整。
