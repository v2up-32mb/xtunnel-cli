package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"x-tunnel/common"
)

const (
	PairStateReady = iota
	PairStateDraining
	PairStateClosed
)

// HotChannelPair 表示一个热通道对
type HotChannelPair struct {
	ID           string
	UplinkChID   int
	DownlinkChID int
	state        int32
	createdAt    time.Time
	refs         int32
}

// State 返回当前状态
func (p *HotChannelPair) State() int { return int(atomic.LoadInt32(&p.state)) }

func (p *HotChannelPair) setState(s int) { atomic.StoreInt32(&p.state, int32(s)) }

// SetStateForTest 仅用于测试设置状态
func (p *HotChannelPair) SetStateForTest(s int) { p.setState(s) }

// PairWarmerConfig PairWarmer 配置
type PairWarmerConfig struct {
	PairCount       int
	RefreshInterval time.Duration
	PrebindTimeout  time.Duration
}

// PairWarmer 热通道对管理器
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

// prebindResult 预绑定结果
type prebindResult struct {
	connID       string
	uplinkChID   int
	downlinkChID int
	err          error
}

// NewPairWarmer 创建新的 PairWarmer
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

// AcquirePrimary 获取一个 Ready 状态的 Pair 并增加引用计数。
// 优先返回当前 primary；若 primary 不可用，则扫描 pairs 列表。
func (w *PairWarmer) AcquirePrimary() *HotChannelPair {
	w.mu.RLock()
	candidates := make([]*HotChannelPair, 0, len(w.pairs))
	if w.primary != nil && w.primary.State() == PairStateReady {
		candidates = append(candidates, w.primary)
	}
	for _, pair := range w.pairs {
		if pair != w.primary && pair.State() == PairStateReady {
			candidates = append(candidates, pair)
		}
	}
	w.mu.RUnlock()

	for _, pair := range candidates {
		atomic.AddInt32(&pair.refs, 1)
		if pair.State() == PairStateReady {
			return pair
		}
		atomic.AddInt32(&pair.refs, -1)
	}
	return nil
}

// ReleasePair 减少 Pair 引用计数。
// Pair 本身保持 Ready 供后续请求复用；只有处于 Draining 状态且 refs 归零时才会移除。
func (w *PairWarmer) ReleasePair(pair *HotChannelPair) {
	if pair == nil {
		return
	}

	refs := atomic.AddInt32(&pair.refs, -1)
	if refs <= 0 && pair.State() == PairStateDraining && pair.State() != PairStateClosed {
		pair.setState(PairStateClosed)
		w.mu.Lock()
		w.removePair(pair)
		w.mu.Unlock()
	}
}

// removePair 从池中移除 Pair（调用者需持有 mu.Lock）
func (w *PairWarmer) removePair(pair *HotChannelPair) {
	for i := 0; i < len(w.pairs); i++ {
		if w.pairs[i] == pair {
			w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
			i--
		}
	}
	if w.primary == pair {
		w.primary = nil
	}
}

// InvalidateChannel 废弃包含指定通道的所有 Pair。
// 将 Pair 标记为 Draining；若当前无请求使用（refs<=0）则立即移除，否则等待使用方释放。
func (w *PairWarmer) InvalidateChannel(chID int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 0; i < len(w.pairs); i++ {
		pair := w.pairs[i]
		if pair.State() == PairStateClosed {
			continue
		}
		if pair.UplinkChID == chID || pair.DownlinkChID == chID {
			if pair.State() != PairStateDraining {
				pair.setState(PairStateDraining)
				log.Printf("[PairWarmer] Pair %s 因通道 %d 失效进入 Draining", pair.ID, chID)
			}
			if atomic.LoadInt32(&pair.refs) <= 0 {
				pair.setState(PairStateClosed)
				w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
				i--
				if w.primary == pair {
					w.primary = nil
				}
			}
		}
	}
	w.ensurePrimaryLocked()
}

// ensurePrimaryLocked 在 primary 为 nil 或不可用时，从 Ready 的 Pair 中选举新的 primary。
// 调用者需持有 mu.Lock。
func (w *PairWarmer) ensurePrimaryLocked() {
	if w.primary != nil && w.primary.State() == PairStateReady {
		return
	}
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			w.primary = pair
			return
		}
	}
	w.primary = nil
}

// SetPrimaryForTest 仅用于测试设置主 Pair
func (w *PairWarmer) SetPrimaryForTest(pair *HotChannelPair) {
	w.mu.Lock()
	w.primary = pair
	w.mu.Unlock()
}

// PairCountForTest 仅用于测试返回 Pair 数量
func (w *PairWarmer) PairCountForTest() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.pairs)
}

// deletePrebindState 删除预绑定临时状态（不调用 Unregister，避免重复加锁和额外日志）
func (w *PairWarmer) deletePrebindState(connID string) {
	w.pool.mu.Lock()
	st := w.pool.conns[connID]
	if st != nil {
		st.closed = true
		delete(w.pool.conns, connID)
	}
	w.pool.mu.Unlock()
}

// BuildPair 使用可用通道列表构建一个 Hot Pair，同步等待预绑定结果
func (w *PairWarmer) BuildPair(available []int) (*HotChannelPair, error) {
	if w.ctx.Err() != nil {
		return nil, fmt.Errorf("PairWarmer 已关闭")
	}
	if len(available) < 2 {
		return nil, fmt.Errorf("可用通道不足，需要至少 2 个通道")
	}

	connID := "prebind-" + uuid.New().String()

	// 在客户端连接池中注册临时状态，使 handleChannel 的 selectDownlink 能正常竞争下行通道。
	w.pool.mu.Lock()
	w.pool.conns[connID] = &clientConnState{
		id:        connID,
		target:    common.PrebindTarget,
		start:     time.Now(),
		connected: make(chan bool, 1),
		closed:    false,
	}
	w.pool.mu.Unlock()

	meta := make([]byte, 1+len(common.PrebindTarget))
	meta[0] = byte(w.pool.config.IPStrategy)
	copy(meta[1:], common.PrebindTarget)

	msg := common.EncodeMessage(common.MsgPrebindRequest, connID, meta, nil)

	// 广播到可用通道
	sent := 0
	for _, chID := range available {
		if err := w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg); err != nil {
			// 记录日志但继续其他通道
			log.Printf("[PairWarmer] 预绑定请求发送到通道 %d 失败: %v", chID, err)
		} else {
			sent++
		}
	}
	if sent == 0 {
		w.deletePrebindState(connID)
		return nil, fmt.Errorf("无法发送预绑定请求到任何可用通道")
	}

	// 等待预绑定结果
	timer := time.NewTimer(w.config.PrebindTimeout)
	defer timer.Stop()

	for {
		select {
		case <-w.ctx.Done():
			w.deletePrebindState(connID)
			return nil, fmt.Errorf("PairWarmer 已关闭")
		case <-timer.C:
			w.deletePrebindState(connID)
			return nil, fmt.Errorf("预绑定超时")
		case res := <-w.prebindResultCh:
			if res.connID == connID {
				if res.err != nil {
					w.deletePrebindState(connID)
					return nil, res.err
				}
				if w.ctx.Err() != nil {
					w.deletePrebindState(connID)
					return nil, fmt.Errorf("PairWarmer 已关闭")
				}
				pair := &HotChannelPair{
					ID:           connID,
					UplinkChID:   res.uplinkChID,
					DownlinkChID: res.downlinkChID,
					state:        int32(PairStateReady),
					createdAt:    time.Now(),
				}
				w.mu.Lock()
				w.pairs = append(w.pairs, pair)
				if w.primary == nil || w.primary.State() != PairStateReady {
					w.primary = pair
				}
				w.mu.Unlock()
				// 预绑定成功，清理临时连接状态
				w.deletePrebindState(connID)
				return pair, nil
			}
			// 不匹配的 connID 忽略，继续等待
		}
	}
}

// HandlePrebindResult 由 clientPool.handleChannel 在收到 MsgSelectUplink 时调用
func (w *PairWarmer) HandlePrebindResult(connID string, uplinkChID, downlinkChID int, err error) {
	res := prebindResult{
		connID:       connID,
		uplinkChID:   uplinkChID,
		downlinkChID: downlinkChID,
		err:          err,
	}
	select {
	case w.prebindResultCh <- res:
	default:
	}
}

// Run 启动 PairWarmer 主循环，监听通道就绪/失效通知并构建/刷新 Pair
func (w *PairWarmer) Run() {
	log.Printf("[PairWarmer] 启动运行循环")
	defer log.Printf("[PairWarmer] 运行循环已退出")

	var refreshTicker *time.Ticker
	if w.config.RefreshInterval > 0 {
		refreshTicker = time.NewTicker(w.config.RefreshInterval)
		defer refreshTicker.Stop()
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case chID := <-w.pool.chReadyCh:
			w.tryBuildPairs()
			_ = chID // 日志中可记录，但当前版本不依赖具体 chID
		case chID := <-w.pool.chInvalidCh:
			w.tryRefresh()
			_ = chID
		case <-func() <-chan time.Time {
			if refreshTicker == nil {
				return nil
			}
			return refreshTicker.C
		}():
			w.periodicRefresh()
		}
	}
}

// tryBuildPairs 尝试构建 Hot Pair，直到 Ready 的 Pair 数量达到 PairCount
func (w *PairWarmer) tryBuildPairs() {
	readyCount := 0
	w.mu.RLock()
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			readyCount++
		}
	}
	w.mu.RUnlock()

	if readyCount >= w.config.PairCount {
		return
	}

	available := w.pool.availableChannels()
	if len(available) < 2 {
		return
	}

	for readyCount < w.config.PairCount {
		pair, err := w.BuildPair(available)
		if err != nil {
			log.Printf("[PairWarmer] 构建 Pair 失败: %v", err)
			return
		}
		log.Printf("[PairWarmer] 成功构建 Pair %s (上行: %d, 下行: %d)", pair.ID, pair.UplinkChID, pair.DownlinkChID)
		readyCount++
	}
}

// tryRefresh 尝试刷新 Pair，当 primary 不可用时触发重建
func (w *PairWarmer) tryRefresh() {
	w.mu.Lock()
	if w.primary == nil || w.primary.State() != PairStateReady {
		w.ensurePrimaryLocked()
	}
	w.mu.Unlock()
	w.tryBuildPairs()
}

// periodicRefresh 周期性刷新：评估当前 Pair 状态并尝试补充 Pair 数量
func (w *PairWarmer) periodicRefresh() {
	w.mu.RLock()
	var primaryID string
	if w.primary != nil {
		primaryID = w.primary.ID
	}
	readyCount := 0
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			readyCount++
		}
	}
	w.mu.RUnlock()

	log.Printf("[PairWarmer] 周期性刷新: 当前 Ready Pair 数量 %d/%d, primary=%s", readyCount, w.config.PairCount, primaryID)

	w.tryBuildPairs()
}
