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

// ReleasePair 减少 Pair 引用计数；若 refs 归零则标记 closed 并从池中移除
func (w *PairWarmer) ReleasePair(pair *HotChannelPair) {
	if pair == nil {
		return
	}

	refs := atomic.AddInt32(&pair.refs, -1)
	if refs <= 0 && pair.State() != PairStateClosed {
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

// InvalidateChannel 废弃包含指定通道的所有 Pair
func (w *PairWarmer) InvalidateChannel(chID int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 0; i < len(w.pairs); i++ {
		pair := w.pairs[i]
		if pair.State() == PairStateClosed {
			continue
		}
		if pair.UplinkChID == chID || pair.DownlinkChID == chID {
			pair.setState(PairStateDraining)
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

// BuildPair 使用可用通道列表构建一个 Hot Pair，同步等待预绑定结果
func (w *PairWarmer) BuildPair(available []int) (*HotChannelPair, error) {
	if len(available) < 2 {
		return nil, fmt.Errorf("可用通道不足，需要至少 2 个通道")
	}

	connID := "prebind-" + uuid.New().String()

	meta := make([]byte, 1+len(common.PrebindTarget))
	meta[0] = byte(w.pool.config.IPStrategy)
	copy(meta[1:], common.PrebindTarget)

	msg := common.EncodeMessage(common.MsgPrebindRequest, connID, meta, nil)

	// 广播到可用通道
	for _, chID := range available {
		if err := w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg); err != nil {
			// 记录日志但继续其他通道
			log.Printf("[PairWarmer] 预绑定请求发送到通道 %d 失败: %v", chID, err)
		}
	}

	// 等待预绑定结果
	timer := time.NewTimer(w.config.PrebindTimeout)
	defer timer.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return nil, fmt.Errorf("PairWarmer 已关闭")
		case <-timer.C:
			return nil, fmt.Errorf("预绑定超时")
		case res := <-w.prebindResultCh:
			if res.connID == connID {
				if res.err != nil {
					return nil, res.err
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
				if w.primary == nil {
					w.primary = pair
				}
				w.mu.Unlock()
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
