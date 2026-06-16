# Hot Channel Pair 与快速重连实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 x-tunnel 客户端与服务端实现 Hot Channel Pair（预绑定上下行通道对）和快速重连/节点切换，以降低 SOCKS5/HTTP 代理首帧延迟并提升链路稳定性。

**Architecture:** 复用现有 `MsgTCPConnect/MsgSelectUplink/MsgSelectDownlink` 竞争逻辑，新增独立的 `MsgPrebindRequest` 用于预绑定；客户端维护 Hot Pair 池，代理请求到达时直接复用已预热通道；叠加 fast retry 状态机与动态测速频率提升稳定性。

**Tech Stack:** Go 1.21+, gorilla/websocket, 现有 x-tunnel 包结构 (common/, client/pkg/, server/pkg/)

---

## 文件结构映射

| 文件 | 职责 |
|------|------|
| `common/protocol.go` | 新增 `MsgPrebindRequest`、`MsgChannelReset`、`PrebindTarget` 常量 |
| `common/protocol_test.go` | 新消息类型与预绑定目标常量测试 |
| `server/pkg/handler.go` | `handlePrebindRequest` 实现 |
| `server/pkg/connection.go` | `MsgChannelReset` 发送；写队列满检测 |
| `server/pkg/pool.go` | `handleMessage` 路由；`sendDownlink` 已支持预绑定复用 |
| `server/pkg/pool_test.go` | 服务端预绑定单元测试；p.conns 不泄漏测试 |
| `client/pkg/config.go` | 新增 Hot Pair / fast retry 配置字段与默认值 |
| `client/pkg/pair_warmer.go` | **新增**：Pair Warmer、HotChannelPair、预绑定握手、生命周期管理 |
| `client/pkg/pair_warmer_test.go` | **新增**：Pair Warmer 单元测试 |
| `client/pkg/pool.go` | 集成 PairWarmer；通道就绪/失效通知；`clientConnState.pair` 字段；`RegisterAndBroadcastTCP` Hot Pair 路径；`handleChannel` 处理 `MsgChannelReset`；`dialAndServe` fast retry 状态机 |
| `client/pkg/pool_test.go` | Hot Pair 集成测试；fast retry 测试；兼容性退化测试 |
| `client/pkg/relay.go` | 动态测速频率调整；`healthScore` |
| `client/pkg/relay_test.go` | 健康分数与测速间隔测试 |
| `client/cmd/x-tunnel-client/main.go` | 新增命令行参数 |

---

## Chunk 1: 协议扩展（common/protocol.go）

### Task 1.1: 新增消息类型与预绑定目标常量

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

func TestPrebindTargetConstant(t *testing.T) {
    if common.PrebindTarget != "x-tunnel.prebind" {
        t.Fatalf("PrebindTarget = %q, want x-tunnel.prebind", common.PrebindTarget)
    }
}
```

Run: `go test ./common -run TestMessageTypeHasPrebindAndReset -v`
Expected: FAIL (undefined: common.MsgPrebindRequest)

- [ ] **Step 2: 实现常量**

在 `common/protocol.go` 中：

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

// PrebindTarget 是预绑定请求中使用的占位目标地址
const PrebindTarget = "x-tunnel.prebind"
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./common -run 'TestMessageTypeHasPrebindAndReset|TestPrebindTargetConstant' -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add common/protocol.go common/protocol_test.go
git commit -m "feat(protocol): 新增 MsgPrebindRequest、MsgChannelReset 与 PrebindTarget

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 2: 服务端预绑定支持

### Task 2.1: handleMessage 路由与 handlePrebindRequest 实现

**Files:**
- Modify: `server/pkg/pool.go:214-238`
- Modify: `server/pkg/handler.go`
- Test: `server/pkg/pool_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHandlePrebindRequestCleansUpState(t *testing.T) {
    p := newTestServerPool()
    connID := "prebind-test-1"
    meta := []byte{0}
    meta = append(meta, common.PrebindTarget...)

    p.handleMessage(1, 10, common.MsgPrebindRequest, connID, meta, nil)

    p.mu.RLock()
    _, exists := p.conns[connID]
    p.mu.RUnlock()
    if exists {
        t.Fatal("prebind connID should be cleaned up")
    }
}
```

Run: `go test ./server/pkg -run TestHandlePrebindRequestCleansUpState -v`
Expected: FAIL（MsgPrebindRequest 未处理，p.conns 仍保留或不存在）

- [ ] **Step 2: 实现 handleMessage 路由**

修改 `server/pkg/pool.go` 的 `handleMessage`：

```go
func (p *serverPool) handleMessage(chID int, rawLen int, msgType common.MessageType, connID string, meta, payload []byte) {
    p.addReceivedBytes(rawLen)
    switch msgType {
    case common.MsgTCPConnect:
        p.handleTCPConnect(chID, connID, meta)

    case common.MsgPrebindRequest:
        p.handlePrebindRequest(chID, connID, meta)

    // ... 其他分支不变
    }
}
```

- [ ] **Step 3: 实现 handlePrebindRequest**

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

- [ ] **Step 4: 运行测试通过**

Run: `go test ./server/pkg -run TestHandlePrebindRequestCleansUpState -v`
Expected: PASS

- [ ] **Step 5: 验证预绑定不泄漏 p.conns**

```go
func TestPrebindDoesNotLeakConns(t *testing.T) {
    p := newTestServerPool()
    meta := []byte{0}
    meta = append(meta, common.PrebindTarget...)
    for i := 0; i < 1000; i++ {
        connID := fmt.Sprintf("prebind-%d", i)
        p.handleMessage(1, 10, common.MsgPrebindRequest, connID, meta, nil)
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

- [ ] **Step 6: 提交**

```bash
git add server/pkg/pool.go server/pkg/handler.go server/pkg/pool_test.go
git commit -m "feat(server): 实现 MsgPrebindRequest 处理并立即清理预绑定状态

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 3: 服务端 MsgChannelReset 发送

### Task 3.1: 实现 notifyChannelReset 与队列满检测

**Files:**
- Modify: `server/pkg/connection.go`
- Create: `server/pkg/connection_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestAsyncWriteQueueFullSendsChannelReset(t *testing.T) {
    p := newTestServerPool()
    wsConn := &ServerWSConn{
        pool:      p,
        chID:      1,
        writeChan: make(chan writeTask, 0),
    }
    resetSent := false
    wsConn.notifyChannelReset = func() error { resetSent = true; return nil }

    for i := 0; i < 3; i++ {
        _ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))
    }
    if !resetSent {
        t.Fatal("MsgChannelReset was not triggered after 3 queue-full events")
    }
}
```

Run: `go test ./server/pkg -run TestAsyncWriteQueueFullSendsChannelReset -v`
Expected: FAIL（notifyChannelReset 方法不存在）

- [ ] **Step 2: 实现 notifyChannelReset 与队列满计数**

在 `ServerWSConn` 中增加：

```go
type ServerWSConn struct {
    // ... 现有字段
    queueFullCount int
    lastQueueFull  time.Time
}
```

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

修改 `asyncWrite` 的 `default` 分支（加锁访问计数器， unlock 在计数判断之后）：

```go
default:
    wsConn.pool.rollbackQueueBytes(size)

    now := time.Now()
    if wsConn.lastQueueFull.IsZero() || now.Sub(wsConn.lastQueueFull) > time.Second {
        wsConn.queueFullCount = 0
    }
    wsConn.lastQueueFull = now
    wsConn.queueFullCount++
    shouldReset := wsConn.queueFullCount >= 3
    if shouldReset {
        wsConn.queueFullCount = 0
    }
    wsConn.mu.Unlock()

    if shouldReset {
        _ = wsConn.notifyChannelReset()
    }
    return fmt.Errorf("写队列满")
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./server/pkg -run TestAsyncWriteQueueFullSendsChannelReset -v`
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
- Create: `client/pkg/config_test.go`

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
    if cfg.FastRetryAttempts != 1 {
        t.Fatalf("FastRetryAttempts default = %d, want 1", cfg.FastRetryAttempts)
    }
    if cfg.FastRetryWindow != 1*time.Second {
        t.Fatalf("FastRetryWindow = %v, want 1s", cfg.FastRetryWindow)
    }
    if cfg.MaxFastRetryConsecutive != 3 {
        t.Fatalf("MaxFastRetryConsecutive default = %d, want 3", cfg.MaxFastRetryConsecutive)
    }
}
```

Run: `go test ./client/pkg -run TestDefaultConfigHasHotPairDefaults -v`
Expected: FAIL（字段不存在）

- [ ] **Step 2: 实现配置字段**

修改 `client/pkg/config.go`：

```go
type Config struct {
    // ... 现有字段

    // Hot Pair 配置
    EnableHotPair          bool          // 是否启用热通道对
    HotPairCount           int           // Hot Pair 数量，默认 1
    HotPairRefreshInterval time.Duration // Pair 刷新间隔，默认 30s

    // 快速重连配置
    FastRetryAttempts       int           // 快速重试次数，默认 1
    FastRetryWindow         time.Duration // 快速重试窗口，默认 1s
    MaxFastRetryConsecutive int           // 连续进入 fast retry 的最大次数，默认 3
}
```

在 `DefaultConfig()` 中增加：

```go
func DefaultConfig() *Config {
    return &Config{
        // ... 现有默认值
        EnableHotPair:           false,
        HotPairCount:            1,
        HotPairRefreshInterval:  30 * time.Second,
        FastRetryAttempts:       1,
        FastRetryWindow:         1 * time.Second,
        MaxFastRetryConsecutive: 3,
    }
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestDefaultConfigHasHotPairDefaults -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/config.go client/pkg/config_test.go
git commit -m "feat(client/config): 新增 Hot Pair 与 fast retry 配置字段

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 4.2: 命令行参数

**Files:**
- Modify: `client/cmd/x-tunnel-client/main.go`

- [ ] **Step 1: 实现参数注册与解析**

在 `registerFlags` 中新增：

```go
fs.BoolVar(&enableHotPair, "hotpair", false, "启用 Hot Channel Pair 降低首帧延迟")
fs.IntVar(&hotPairCount, "hotpair-count", 1, "Hot Pair 数量")
fs.DurationVar(&hotPairRefreshInterval, "hotpair-refresh", 30*time.Second, "Hot Pair 刷新间隔")
fs.IntVar(&fastRetryAttempts, "fast-retry", 1, "快速重试次数")
fs.DurationVar(&fastRetryWindow, "fast-retry-window", 1*time.Second, "快速重试窗口")
fs.IntVar(&maxFastRetryConsecutive, "fast-retry-consecutive", 3, "连续进入快速重试的最大次数")
```

新增包级变量并赋值：

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

在 `parseFlags` 中：

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
- Create: `client/pkg/pair_warmer_test.go`

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
    state        int32
    createdAt    time.Time
    refs         int32
}

func (p *HotChannelPair) State() int {
    return int(atomic.LoadInt32(&p.state))
}

func (p *HotChannelPair) setState(s int) {
    atomic.StoreInt32(&p.state, int32(s))
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

    prebindResultCh chan prebindResult
}

type prebindResult struct {
    connID       string
    uplinkChID   int
    downlinkChID int
    err          error
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
        ctx:             ctx,
        cancel:          cancel,
        prebindResultCh: make(chan prebindResult, 8),
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

### Task 5.2: 实现 AcquirePrimary / Release / Invalidate / CloseIfEmpty

**Files:**
- Modify: `client/pkg/pair_warmer.go`
- Test: `client/pkg/pair_warmer_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestPairWarmerAcquireReleaseAndClose(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    pool := &clientPool{config: cfg}
    warmer := client.NewPairWarmer(pool, cfg)

    pair := &client.HotChannelPair{ID: "p1", UplinkChID: 1, DownlinkChID: 2}
    pair.SetStateForTest(client.PairStateReady)
    warmer.SetPrimaryForTest(pair)

    acquired := warmer.AcquirePrimary()
    if acquired == nil {
        t.Fatal("AcquirePrimary returned nil")
    }
    if atomic.LoadInt32(&acquired.refs) != 1 {
        t.Fatalf("refs = %d, want 1", atomic.LoadInt32(&acquired.refs))
    }

    warmer.ReleasePair(acquired)
    if atomic.LoadInt32(&acquired.refs) != 0 {
        t.Fatalf("refs after release = %d, want 0", atomic.LoadInt32(&acquired.refs))
    }
    if acquired.State() != client.PairStateClosed {
        t.Fatalf("state = %d, want closed", acquired.State())
    }
}
```

Run: `go test ./client/pkg -run TestPairWarmerAcquireReleaseAndClose -v`
Expected: FAIL（方法不存在）

- [ ] **Step 2: 实现方法**

在 `pair_warmer.go` 中新增：

```go
// AcquirePrimary 获取当前主 Pair 并增加引用计数
func (w *PairWarmer) AcquirePrimary() *HotChannelPair {
    w.mu.RLock()
    pair := w.primary
    w.mu.RUnlock()
    if pair == nil || pair.State() != PairStateReady {
        return nil
    }
    atomic.AddInt32(&pair.refs, 1)
    return pair
}

// ReleasePair 减少 Pair 引用计数；若 Pair 已 draining 且 refs 归零则标记 closed
func (w *PairWarmer) ReleasePair(pair *HotChannelPair) {
    if pair == nil {
        return
    }
    refs := atomic.AddInt32(&pair.refs, -1)
    if refs <= 0 && pair.State() == PairStateDraining {
        pair.setState(PairStateClosed)
        w.removePair(pair)
    }
}

// removePair 从池中移除 Pair
func (w *PairWarmer) removePair(pair *HotChannelPair) {
    w.mu.Lock()
    defer w.mu.Unlock()
    for i, p := range w.pairs {
        if p == pair {
            w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
            break
        }
    }
    if w.primary == pair {
        w.primary = nil
    }
}

// InvalidateChannel 废弃包含指定通道的所有 Pair
func (w *PairWarmer) InvalidateChannel(chID int) {
    w.mu.Lock()
    defer w.mu.Unlock()
    for _, pair := range w.pairs {
        if pair.State() == PairStateClosed {
            continue
        }
        if pair.UplinkChID == chID || pair.DownlinkChID == chID {
            pair.setState(PairStateDraining)
            if pair.refs <= 0 {
                pair.setState(PairStateClosed)
                w.removePair(pair)
            } else if w.primary == pair {
                w.primary = nil
            }
        }
    }
}

// 仅用于测试的辅助方法
func (p *HotChannelPair) SetStateForTest(s int) { p.setState(s) }

func (w *PairWarmer) SetPrimaryForTest(pair *HotChannelPair) {
    w.mu.Lock()
    w.primary = pair
    w.pairs = append(w.pairs, pair)
    w.mu.Unlock()
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestPairWarmerAcquireReleaseAndClose -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pair_warmer_test.go
git commit -m "feat(client): 实现 PairWarmer 生命周期管理（Acquire/Release/Invalidate）

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 5.3: 实现完整 BuildPair 与结果回调

**Files:**
- Modify: `client/pkg/pair_warmer.go`
- Modify: `client/pkg/pool.go`（handleChannel 转发预绑定结果）
- Test: `client/pkg/pair_warmer_test.go`

- [ ] **Step 1: 写失败测试**

```go
func TestPairWarmerBuildPairReturnsOnResult(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    pool := newTestClientPool(cfg)
    warmer := client.NewPairWarmer(pool, cfg)

    go func() {
        time.Sleep(10 * time.Millisecond)
        warmer.HandlePrebindResult("prebind-x", 1, 2, nil)
    }()

    pair, err := warmer.BuildPair([]int{1, 2})
    if err != nil {
        t.Fatalf("BuildPair failed: %v", err)
    }
    if pair.UplinkChID != 1 || pair.DownlinkChID != 2 {
        t.Fatalf("unexpected pair: %+v", pair)
    }
}
```

Run: `go test ./client/pkg -run TestPairWarmerBuildPairReturnsOnResult -v`
Expected: FAIL（方法不存在）

- [ ] **Step 2: 实现 BuildPair 与 HandlePrebindResult**

在 `pair_warmer.go` 中新增：

```go
import (
    "encoding/binary"
    "fmt"
    "github.com/google/uuid"
    "github.com/gorilla/websocket"
    "x-tunnel/common"
)

// BuildPair 使用可用通道列表构建一个 Hot Pair，同步等待预绑定结果
func (w *PairWarmer) BuildPair(available []int) (*HotChannelPair, error) {
    if len(available) < 2 {
        return nil, fmt.Errorf("可用通道不足")
    }

    connID := "prebind-" + uuid.New().String()
    meta := make([]byte, 1+len(common.PrebindTarget))
    meta[0] = byte(w.pool.config.IPStrategy)
    copy(meta[1:], common.PrebindTarget)

    msg := common.EncodeMessage(common.MsgPrebindRequest, connID, meta, nil)
    for _, chID := range available {
        _ = w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg)
    }

    timer := time.NewTimer(w.config.PrebindTimeout)
    defer timer.Stop()

    for {
        select {
        case <-w.ctx.Done():
            return nil, w.ctx.Err()
        case <-timer.C:
            return nil, fmt.Errorf("预绑定超时")
        case res := <-w.prebindResultCh:
            if res.connID != connID {
                // 延迟的或无关的结果，继续等待
                continue
            }
            if res.err != nil {
                return nil, res.err
            }
            pair := &HotChannelPair{
                ID:           connID,
                UplinkChID:   res.uplinkChID,
                DownlinkChID: res.downlinkChID,
                createdAt:    time.Now(),
            }
            pair.setState(PairStateReady)
            return pair, nil
        }
    }
}

// HandlePrebindResult 由 clientPool.handleChannel 在收到 MsgSelectUplink 时调用
func (w *PairWarmer) HandlePrebindResult(connID string, uplinkChID, downlinkChID int, err error) {
    select {
    case w.prebindResultCh <- prebindResult{connID: connID, uplinkChID: uplinkChID, downlinkChID: downlinkChID, err: err}:
    default:
    }
}
```

- [ ] **Step 3: 在 pool.handleChannel 中转发预绑定结果**

修改 `client/pkg/pool.go` 的 `handleChannel`，在 `common.MsgSelectUplink` 分支中：

```go
case common.MsgSelectUplink:
    var uplinkChID int
    if len(meta) >= 4 {
        uplinkChID = int(binary.BigEndian.Uint32(meta[0:4]))
    } else {
        uplinkChID = chID
    }
    p.noteUplink(connID, uplinkChID)

    selected, _, _, _, _, _ := p.selectDownlink(connID, chID)
    if selected {
        chosen := int(atomic.LoadInt32(&p.conns[connID].downlink))
        downlinkBytes := make([]byte, 4)
        binary.BigEndian.PutUint32(downlinkBytes, uint32(chID))
        _ = p.asyncWriteDirect(uplinkChID, websocket.BinaryMessage, common.EncodeMessage(common.MsgSelectDownlink, connID, downlinkBytes, nil))

        // 如果是预绑定请求，通知 PairWarmer
        if p.pairWarmer != nil && strings.HasPrefix(connID, "prebind-") {
            p.pairWarmer.HandlePrebindResult(connID, uplinkChID, chID, nil)
        }
    }
```

注意：当前 `selectDownlink` 会竞争设置 downlink，预绑定结果需要知道哪个 chID 赢得了下行。`chID` 是当前处理通道，即最快收到 MsgSelectUplink 的通道，因此下行通道就是 `chID`。

- [ ] **Step 4: 运行测试通过**

Run: `go test ./client/pkg -run TestPairWarmerBuildPairReturnsOnResult -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pool.go client/pkg/pair_warmer_test.go
git commit -m "feat(client): 实现完整 BuildPair 与预绑定结果回调

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 6: 客户端 pool 集成 PairWarmer

### Task 6.1: 增加通道就绪/失效通知与 clientConnState.pair 字段

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestClientPoolHasChannelNotificationChannels(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    if p.chReadyCh == nil || p.chInvalidCh == nil {
        t.Fatal("notification channels should not be nil")
    }
    if cap(p.chReadyCh) == 0 || cap(p.chInvalidCh) == 0 {
        t.Fatal("notification channels should be buffered")
    }
}
```

Run: `go test ./client/pkg -run TestClientPoolHasChannelNotificationChannels -v`
Expected: FAIL（字段不存在）

- [ ] **Step 2: 实现字段**

修改 `clientConnState`：

```go
type clientConnState struct {
    // ... 现有字段
    pair *HotChannelPair
}
```

修改 `clientPool`：

```go
type clientPool struct {
    // ... 现有字段
    pairWarmer  *PairWarmer
    chReadyCh   chan int
    chInvalidCh chan int
}
```

在 `newClientPool` 中：

```go
p := &clientPool{
    // ... 现有初始化
    chReadyCh:   make(chan int, 64),
    chInvalidCh: make(chan int, 64),
}
if cfg.EnableHotPair {
    p.pairWarmer = NewPairWarmer(p, cfg)
}
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestClientPoolHasChannelNotificationChannels -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): 增加通道通知字段与 clientConnState.pair

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

- [ ] **Step 2: 实现失效通知**

在 `dialAndServe` 中，连接断开后、重连前：

```go
// 在 handleChannel 返回后，cleanupChannel 之前
select {
case p.chInvalidCh <- chID:
default:
}
if p.pairWarmer != nil {
    p.pairWarmer.InvalidateChannel(chID)
}
```

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
func TestRegisterAndBroadcastTCPFallsBackWhenNoPair(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    // 无 Pair 时退化到广播路径
    p.RegisterAndBroadcastTCP("real-conn-1", "example.com:80", nil, nil, "TEST")
    // 验证通过 broadcastWrite 发送（可 mock 或统计）
}
```

Run: `go test ./client/pkg -run TestRegisterAndBroadcastTCPFallsBackWhenNoPair -v`
Expected: 根据测试实现而定

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
        if pair != nil {
            p.mu.Lock()
            st = p.conns[connID]
            if st != nil {
                st.pair = pair
            }
            p.mu.Unlock()
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

- [ ] **Step 3: Unregister 释放 Pair refs**

修改 `Unregister` 开头：

```go
func (p *clientPool) Unregister(connID string) {
    p.mu.Lock()
    st := p.conns[connID]
    // ... 现有逻辑直到 delete
    var pair *HotChannelPair
    if st != nil {
        pair = st.pair
        st.pair = nil
    }
    p.mu.Unlock()

    if pair != nil && p.pairWarmer != nil {
        p.pairWarmer.ReleasePair(pair)
    }

    // ... 后续关闭逻辑
}
```

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): RegisterAndBroadcastTCP 支持 Hot Pair 路径与 fallback

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.4: handleChannel 处理 MsgChannelReset

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestHandleChannelChannelResetInvalidatesPair(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    // 构造一个 Ready 状态的 Pair
    pair := &client.HotChannelPair{ID: "p1", UplinkChID: 1, DownlinkChID: 2}
    pair.SetStateForTest(client.PairStateReady)
    p.pairWarmer.SetPrimaryForTest(pair)

    meta := make([]byte, 4)
    binary.BigEndian.PutUint32(meta, uint32(1))
    p.handleChannel(1, common.EncodeMessage(common.MsgChannelReset, "", meta, nil))

    if pair.State() != client.PairStateDraining {
        t.Fatalf("pair state = %d, want draining", pair.State())
    }
}
```

Run: `go test ./client/pkg -run TestHandleChannelChannelResetInvalidatesPair -v`
Expected: FAIL（handleChannel 未处理 MsgChannelReset）

- [ ] **Step 2: 实现处理分支**

在 `handleChannel` 的 switch 中新增：

```go
case common.MsgChannelReset:
    if len(meta) >= 4 {
        resetChID := int(binary.BigEndian.Uint32(meta[0:4]))
        select {
        case p.chInvalidCh <- resetChID:
        default:
        }
        if p.pairWarmer != nil {
            p.pairWarmer.InvalidateChannel(resetChID)
        }
    }
```

- [ ] **Step 3: 运行测试通过**

Run: `go test ./client/pkg -run TestHandleChannelChannelResetInvalidatesPair -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): handleChannel 处理 MsgChannelReset

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 6.5: 启动 PairWarmer 与 Run 循环

**Files:**
- Modify: `client/pkg/pair_warmer.go`
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 实现 Run 与 tryBuildPairs/tryRefresh**

在 `pair_warmer.go` 中新增：

```go
func (w *PairWarmer) Run() {
    timer := time.NewTimer(w.config.RefreshInterval)
    defer timer.Stop()

    available := make(map[int]bool)

    for {
        select {
        case <-w.ctx.Done():
            return
        case chID := <-w.pool.chReadyCh:
            available[chID] = true
            if w.primary == nil && len(available) >= w.config.PairCount*2 {
                w.tryBuildPairs(available)
            }
        case chID := <-w.pool.chInvalidCh:
            delete(available, chID)
            w.InvalidateChannel(chID)
            w.tryBuildPairs(available)
        case <-timer.C:
            w.tryRefresh(available)
            timer.Reset(w.config.RefreshInterval)
        }
    }
}

func (w *PairWarmer) tryBuildPairs(available map[int]bool) {
    if w.primary != nil && w.primary.State() == PairStateReady {
        return
    }
    if len(available) < w.config.PairCount*2 {
        return
    }
    ids := make([]int, 0, len(available))
    for id := range available {
        ids = append(ids, id)
    }
    pair, err := w.BuildPair(ids)
    if err != nil {
        log.Printf("[PairWarmer] 构建 Pair 失败: %v", err)
        return
    }
    w.mu.Lock()
    w.pairs = append(w.pairs, pair)
    w.primary = pair
    w.mu.Unlock()
    log.Printf("[PairWarmer] 新 Pair 就绪: uplink=%d downlink=%d", pair.UplinkChID, pair.DownlinkChID)
}

func (w *PairWarmer) tryRefresh(available map[int]bool) {
    // TODO: 评估当前 Pair 质量，需要时构建新 Pair
    // 初始版本可仅在没有主 Pair 时重建
    w.tryBuildPairs(available)
}
```

- [ ] **Step 2: 在 pool.Start 中启动 warmer**

```go
if p.config.EnableHotPair && p.pairWarmer != nil {
    go p.pairWarmer.Run()
}
```

- [ ] **Step 3: 运行编译**

Run: `go build ./client/pkg`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add client/pkg/pair_warmer.go client/pkg/pool.go
git commit -m "feat(client): 启动 PairWarmer Run 循环

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Chunk 7: 快速重连状态机

### Task 7.1: dialAndServe 集成 fast retry

**Files:**
- Modify: `client/pkg/pool.go`

- [ ] **Step 1: 写失败测试**

```go
func TestFastRetryStateTransitions(t *testing.T) {
    state := &client.FastRetryState{}
    state.OnFailure()
    state.OnFailure()
    if !state.ShouldFastRetry(3) {
        t.Fatal("should fast retry")
    }
    state.OnFailure()
    if state.ShouldFastRetry(3) {
        t.Fatal("should not fast retry after threshold")
    }
    state.OnSuccess()
    if !state.ShouldFastRetry(3) {
        t.Fatal("should reset and allow fast retry")
    }
}
```

Run: `go test ./client/pkg -run TestFastRetryStateTransitions -v`
Expected: FAIL（FastRetryState 未导出）

调整为未导出类型的测试（在同一包中测试，或提供测试辅助函数）。

- [ ] **Step 2: 实现 FastRetryState**

在 `client/pkg/pool.go` 中新增：

```go
type fastRetryState struct {
    consecutiveFailures int
    lastFailure         time.Time
}

func (f *fastRetryState) OnFailure() {
    f.consecutiveFailures++
    f.lastFailure = time.Now()
}

func (f *fastRetryState) OnSuccess() {
    f.consecutiveFailures = 0
    f.lastFailure = time.Time{}
}

func (f *fastRetryState) ShouldFastRetry(maxConsecutive int) bool {
    return f.consecutiveFailures < maxConsecutive
}
```

- [ ] **Step 3: 修改 dialAndServe**

在 `dialAndServe` 中：

```go
frs := &fastRetryState{}
fastRetryCount := 0

for {
    // ...
    wsConn, err := p.dialWebSocket(chID, ip)
    if err != nil {
        frs.OnFailure()
        if frs.ShouldFastRetry(p.config.MaxFastRetryConsecutive) && fastRetryCount < p.config.FastRetryAttempts {
            fastRetryCount++
            jitter := time.Duration(rand.Intn(300)) * time.Millisecond
            delay := p.config.FastRetryWindow + jitter
            select {
            case <-p.ctx.Done():
                return
            case <-time.After(delay):
                continue
            }
        }
        fastRetryCount = 0
        // 进入指数退避 ...
    }

    // 连接成功
    frs.OnSuccess()
    fastRetryCount = 0
    // ...
}
```

注意：
- 当 `slowRetryMode` 为 true 时，跳过 fast retry。
- fast retry 次数每天窗口内重置（成功或进入指数退避时）。

- [ ] **Step 4: 运行测试通过**

Run: `go test ./client/pkg -run TestFastRetryStateTransitions -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add client/pkg/pool.go client/pkg/pool_test.go
git commit -m "feat(client/pool): 实现 fast retry 状态机并与 dialAndServe 集成

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
    mgr.SetHealthScore(50)
    interval = mgr.CurrentTestInterval()
    if interval != 30*time.Second {
        t.Fatalf("interval = %v, want 30s", interval)
    }
    mgr.SetHealthScore(80)
    interval = mgr.CurrentTestInterval()
    if interval != 60*time.Second {
        t.Fatalf("interval = %v, want 60s", interval)
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

- [ ] **Step 3: 更新 healthScore**

在 `testAllNodes` 末尾调用 `updateHealthScore()`：

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

将 `speedTestLoop` 改为 `time.Timer` 模式，每次循环后重置间隔：

```go
func (m *RelayNodeManager) speedTestLoop() {
    timer := time.NewTimer(m.CurrentTestInterval())
    defer timer.Stop()
    for {
        select {
        case <-m.ctx.Done():
            return
        case <-timer.C:
            m.testAllNodes()
            timer.Reset(m.CurrentTestInterval())
        }
    }
}
```

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

### Task 9.1: 服务端预绑定集成测试

**Files:**
- Test: `server/pkg/pool_test.go`

- [ ] **Step 1: 写测试**

```go
func TestPrebindSendsSelectUplink(t *testing.T) {
    p := newTestServerPool()
    // 创建 mock wsConn 并注册到 p.chConns
    // 发送 MsgPrebindRequest
    // 验证 p.sendDownlink 被调用且携带 MsgSelectUplink
}
```

Run: `go test ./server/pkg -run TestPrebindSendsSelectUplink -v`
Expected: 根据 mock 实现而定

- [ ] **Step 2: 实现 mock helper**

在 `server/pkg/pool_test.go` 中实现可复用的 `newTestServerPool` 与 mock WebSocket 连接（如尚未存在）。

- [ ] **Step 3: 提交**

```bash
git add server/pkg/pool_test.go
git commit -m "test(server): 添加预绑定集成测试

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 9.2: 客户端 Hot Pair 集成与退化测试

**Files:**
- Test: `client/pkg/pool_test.go`

- [ ] **Step 1: 写测试**

```go
func TestHotPairAcquireAndChannelInvalidation(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    pair := &client.HotChannelPair{ID: "p1", UplinkChID: 1, DownlinkChID: 2}
    pair.SetStateForTest(client.PairStateReady)
    p.pairWarmer.SetPrimaryForTest(pair)

    acquired := p.pairWarmer.AcquirePrimary()
    if acquired == nil {
        t.Fatal("should acquire pair")
    }
    p.pairWarmer.InvalidateChannel(1)
    if pair.State() != client.PairStateDraining {
        t.Fatalf("state = %d, want draining", pair.State())
    }
}

func TestRegisterAndBroadcastTCPFallsBackToBroadcast(t *testing.T) {
    cfg := client.DefaultConfig()
    cfg.EnableHotPair = true
    p, _ := newClientPool(cfg, context.Background(), func() {})
    // 未设置 Pair，应走广播路径
    // 验证 broadcastWrite 被调用
}
```

- [ ] **Step 2: 运行测试通过**

Run: `go test ./client/pkg -run 'TestHotPair|TestRegisterAndBroadcast' -v`
Expected: PASS

- [ ] **Step 3: 提交**

```bash
git add client/pkg/pool_test.go
git commit -m "test(client): 添加 Hot Pair 与退化路径测试

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

### Task 9.3: 完整测试与编译

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

Expected: 所有测试 PASS

- [ ] **Step 3: 提交**

```bash
git add .
git commit -m "test: 全量测试通过

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Plan Review Loop

每个 Chunk 完成后，应 dispatch plan-document-reviewer 进行评审，重点检查：

1. 文件路径和修改位置是否正确。
2. 测试是否覆盖了关键风险点（p.conns 泄漏、Pair refs 归零、通道失效、fast retry 阈值、healthScore 区间）。
3. 锁的使用是否安全（特别是 `asyncWrite` 计数器、`pair.state` 原子操作）。
4. 实现顺序是否会产生循环依赖。

如果 reviewer 提出 issues，修复后重新 dispatch，最多 5 次迭代。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-16-hot-channel-pair-fast-reconnect-plan.md`. Ready to execute?

执行路径：
- 优先使用 `superpowers:subagent-driven-development` 分派子代理实现每个 Task。
- 如果没有子代理，使用 `superpowers:executing-plans` 在当前会话分批执行。

每个 Task 完成后运行对应测试并提交，确保 TDD 循环完整。
