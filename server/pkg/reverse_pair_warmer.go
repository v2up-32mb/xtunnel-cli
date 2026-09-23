package server

import (
	"encoding/binary"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// ReverseHotPair 反向模式预热通道对。
// P1 = 服务端发包通道（客户端选定的首达通道，来自 MsgSelectUplink meta）；
// P2 = 服务端收包通道（竞争获胜的 MsgSelectUplink 到达通道）。
type ReverseHotPair struct {
	ID    string // prebind connID
	owner string
	P1    int
	P2    int
	ready int32 // atomic：0=构建中 1=就绪
}

func (p *ReverseHotPair) IsReady() bool { return atomic.LoadInt32(&p.ready) == 1 }

// ReversePairWarmer 反向模式预热器（-hotpair 启用时创建）。
// 镜像正向 clientPool 的 PairWarmer：后台循环为每个已注册反向监听的客户端
// 预构建 {P1,P2} 通道对，使代理请求到达时单播直达、无需重走广播竞争。
type ReversePairWarmer struct {
	pool     *serverPool
	count    int
	interval time.Duration

	mu       sync.Mutex
	pending  map[string]*ReverseHotPair   // connID → 构建中的 pair
	ready    map[string][]*ReverseHotPair // clientID → 就绪 pair 队列
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewReversePairWarmer(pool *serverPool, count int, interval time.Duration) *ReversePairWarmer {
	if count <= 0 {
		count = 1
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ReversePairWarmer{
		pool:     pool,
		count:    count,
		interval: interval,
		pending:  make(map[string]*ReverseHotPair),
		ready:    make(map[string][]*ReverseHotPair),
		stopCh:   make(chan struct{}),
	}
}

// Start 启动后台预热循环
func (w *ReversePairWarmer) Start() {
	go w.run()
}

// Stop 停止预热循环
func (w *ReversePairWarmer) Stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *ReversePairWarmer) run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	w.tryBuildPairs()
	for {
		select {
		case <-ticker.C:
			w.tryBuildPairs()
		case <-w.stopCh:
			return
		}
	}
}

// tryBuildPairs 为每个持有反向监听器的客户端补足预热 Pair
func (w *ReversePairWarmer) tryBuildPairs() {
	if w.pool == nil || w.pool.reverseManager == nil {
		return
	}
	for _, clientID := range w.pool.reverseManager.ClientIDs() {
		if w.pool.countActiveChannelsForClient(clientID) == 0 {
			continue
		}
		w.mu.Lock()
		readyN := len(w.ready[clientID])
		pendingN := 0
		for _, pair := range w.pending {
			if pair.owner == clientID {
				pendingN++
			}
		}
		w.mu.Unlock()
		for i := readyN + pendingN; i < w.count; i++ {
			w.buildPair(clientID)
		}
	}
}

// buildPair 向客户端广播预绑定请求，登记构建中的 Pair
func (w *ReversePairWarmer) buildPair(clientID string) {
	connID := "prebind-" + uuid.NewString()
	pair := &ReverseHotPair{ID: connID, owner: clientID}

	w.mu.Lock()
	w.pending[connID] = pair
	w.mu.Unlock()

	meta := make([]byte, 1+len(protocol.PrebindTarget))
	meta[0] = byte(protocol.IPStrategyDefault)
	copy(meta[1:], protocol.PrebindTarget)
	msg := protocol.EncodeMessage(protocol.MsgPrebindRequest, connID, meta, nil)

	if err := w.pool.broadcastWriteToClient(clientID, websocket.BinaryMessage, msg); err != nil {
		w.mu.Lock()
		delete(w.pending, connID)
		w.mu.Unlock()
		log.Printf("[ReversePairWarmer] 预绑定请求发送失败（客户端 %s）: %v", protocol.ShortID(clientID), err)
		return
	}

	// 超时兜底：构建中的 Pair 若未在刷新周期内完成则丢弃
	time.AfterFunc(w.interval, func() {
		w.mu.Lock()
		cur, ok := w.pending[connID]
		if ok && cur == pair {
			delete(w.pending, connID)
		}
		w.mu.Unlock()
	})
}

// HandlePrebindUplink 处理客户端回的 MsgSelectUplink（handleMessage 前置拦截后进入）。
// 首达竞争：P1 取自 meta，P2 取到达通道；完成即标 Ready。
func (w *ReversePairWarmer) HandlePrebindUplink(clientID string, chID int, connID string, meta []byte) {
	if len(meta) < 4 {
		return
	}
	w.mu.Lock()
	pair, ok := w.pending[connID]
	if !ok || pair.owner != clientID {
		w.mu.Unlock()
		return
	}
	if !atomic.CompareAndSwapInt32(&pair.ready, 0, 1) {
		w.mu.Unlock()
		return // 已完成
	}
	pair.P1 = int(binary.BigEndian.Uint32(meta[:4]))
	pair.P2 = chID
	delete(w.pending, connID)
	w.ready[clientID] = append(w.ready[clientID], pair)
	w.mu.Unlock()
	log.Printf("[ReversePairWarmer] Pair 构建完成 (P1:%d P2:%d)，客户端 %s", pair.P1, pair.P2, protocol.ShortID(clientID))
}

// AcquirePair 取一个就绪 Pair（一次性消费）；无就绪 Pair 返回 nil，调用方回退广播竞争。
func (w *ReversePairWarmer) AcquirePair(clientID string) *ReverseHotPair {
	w.mu.Lock()
	defer w.mu.Unlock()
	list := w.ready[clientID]
	for i, pair := range list {
		if pair.IsReady() {
			w.ready[clientID] = append(list[:i], list[i+1:]...)
			return pair
		}
	}
	return nil
}

// InvalidateChannel 通道断开时废弃绑定该通道的全部 Pair
func (w *ReversePairWarmer) InvalidateChannel(chID int) {
	w.mu.Lock()
	for connID, pair := range w.pending {
		// 构建中的 Pair 尚无通道绑定，仅在客户端通道清空时由 InvalidateClient 处理
		_ = pair
		delete(w.pending, connID)
	}
	for clientID, list := range w.ready {
		kept := list[:0]
		for _, pair := range list {
			if pair.P1 == chID || pair.P2 == chID {
				log.Printf("[ReversePairWarmer] Pair 因通道 %d 断开而废弃 (P1:%d P2:%d)", chID, pair.P1, pair.P2)
				continue
			}
			kept = append(kept, pair)
		}
		if len(kept) == 0 {
			delete(w.ready, clientID)
		} else {
			w.ready[clientID] = kept
		}
	}
	w.mu.Unlock()
}

// InvalidateClient 客户端最后通道断开（或监听器注销）时清空其全部 Pair
func (w *ReversePairWarmer) InvalidateClient(clientID string) {
	w.mu.Lock()
	for connID, pair := range w.pending {
		if pair.owner == clientID {
			delete(w.pending, connID)
		}
	}
	delete(w.ready, clientID)
	w.mu.Unlock()
}

// ClientIDs 提取 ReverseListenerManager 中持有监听器的客户端列表（供预热器消费）
func (m *ReverseListenerManager) ClientIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.clients))
	for id := range m.clients {
		ids = append(ids, id)
	}
	return ids
}

// reversePrebindPrefix 反向预绑定 connID 前缀（与正向 PairWarmer 一致）
const reversePrebindPrefix = "prebind-"

func isReversePrebindConnID(connID string) bool {
	return strings.HasPrefix(connID, reversePrebindPrefix)
}
