package client

// 回归测试：win7-compat 修复 —— 对齐上游 v0.2.1 的 Hot Pair 热路径语义。
// 1. 热路径拨号必须预置上行通道（st.uplink = pair.UplinkChID），否则服务端提升后
//    （不再回发 MsgSelectUplink）客户端 GetUplinkChannel 恒 false，全部上行数据退化为广播。
// 2. SOCKS5 回执写失败早退时必须归还已预取的 Pair 引用，否则 refs 泄漏。
//
// 提交: 与上游 xtunnel@v0.2.1 pool.go:974 / proxy_dialer.go 对齐

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"x-tunnel/common"
)

// readyTestPair 构造一个 Ready 状态的 HotChannelPair（上行=3,下行=1,kep prebind-test）
func readyTestPair() *HotChannelPair {
	return &HotChannelPair{
		ID:           "01",
		Key:          "prebind-test",
		UplinkChID:   3,
		DownlinkChID: 1,
		state:        int32(PairStateReady),
		createdAt:    time.Now(),
	}
}

// newHotPairTestPool 构造启用了 HotPair 的最小 clientPool（3 条通道，写队列已就绪）
func newHotPairTestPool(t *testing.T) *clientPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := DefaultConfig()
	cfg.EnableHotPair = true
	cfg.Connections = 3

	p := &clientPool{
		ctx:              ctx,
		cancel:           cancel,
		config:           cfg,
		conns:            make(map[string]*clientConnState),
		wsConns:          make([]*websocket.Conn, 3),
		writeQueues:      make([]chan writeJob, 3),
		connsWriteMutex:  make([]sync.Mutex, 3),
		globalQueueLimit: 1 << 20,
		nextChannel:      1,
	}
	for i := range p.writeQueues {
		p.writeQueues[i] = make(chan writeJob, 16)
	}

	p.pairWarmer = NewPairWarmer(p, cfg)
	pair := readyTestPair()
	p.pairWarmer.mu.Lock()
	p.pairWarmer.pairs = append(p.pairWarmer.pairs, pair)
	p.pairWarmer.primary = pair
	p.pairWarmer.mu.Unlock()

	return p
}

// TestHotPairRegisterPresetsUplink：热路径注册后 GetUplinkChannel 必须返回 Pair 上行通道，
// 且 MsgTCPConnect 单播进 Pair 上行通道队列（而非广播）。
func TestHotPairRegisterPresetsUplink(t *testing.T) {
	p := newHotPairTestPool(t)
	pair := readyTestPair() // 上行 3、下行 1
	connID := "prebind-test-conn-uuid-1"

	reqType := "SOCKS5"
	p.RegisterAndBroadcastTCP(connID, "example.com:443", nil, nil, reqType, pair)

	// 1) 上行通道已预置，GetUplinkChannel 命中 Pair 上行（3）
	chID, ok := p.GetUplinkChannel(connID)
	if !ok {
		t.Fatal("GetUplinkChannel should return true after hot-path register (st.uplink preset)")
	}
	if chID != pair.UplinkChID {
		t.Fatalf("uplink = %d, want pair.UplinkChID %d", chID, pair.UplinkChID)
	}

	// 2) MsgTCPConnect 仅进入 Pair 上行通道写队列（单播，而非广播到全部通道）
	var got *writeJob
	select {
	case job := <-p.writeQueues[pair.UplinkChID-1]:
		got = &job
	case <-time.After(time.Second):
		t.Fatal("no MsgTCPConnect enqueued on pair uplink channel")
	}
	mt, cid, _, _, err := common.DecodeMessage(got.data)
	if err != nil {
		t.Fatalf("decode enqueued message failed: %v", err)
	}
	if mt != common.MsgTCPConnect || cid != connID {
		t.Fatalf("expected MsgTCPConnect(%s) on uplink queue, got type=%d cid=%s", connID, mt, cid)
	}
	// 其他通道不应收到同一条消息（严格单播）
	for i, q := range p.writeQueues {
		if i == pair.UplinkChID-1 {
			continue
		}
		select {
		case job := <-q:
			mt2, cid2, _, _, _ := common.DecodeMessage(job.data)
			t.Fatalf("unexpected message duplicated to channel %d (type=%d cid=%s)", i+1, mt2, cid2)
		default:
		}
	}
}

// TestHotPairRegisterFallbackClearsUplink：热路径发送失败回退广播时，
// 必须清空预置上行并释放 Pair 引用，残留预置会导致后续广播数据被误导向失效通道。
func TestHotPairRegisterFallbackClearsUplink(t *testing.T) {
	p := newHotPairTestPool(t)
	// 使 Pair 上行通道不可用：清空其写队列并借用已有引用计数
	pair := readyTestPair()
	// 上行通道 3 的写队列置空（不可用）
	p.writeQueues[pair.UplinkChID-1] = nil

	connID := "prebind-test-conn-fallback-uuid"
	p.RegisterAndBroadcastTCP(connID, "example.com:443", nil, nil, "SOCKS5", pair)

	if chID, ok := p.GetUplinkChannel(connID); ok {
		t.Fatalf("after hot-path send failure, GetUplinkChannel should be false, got %d", chID)
	}
}

// TestSOCKS5ReplyWriteFailReleasesPair：回执写失败（本地对端已断开）时，
// Prebits 的 Pair 引用必须归还，refs 回到 0，不产生泄漏。
func TestSOCKS5ReplyWriteFailReleasesPair(t *testing.T) {
	p := newHotPairTestPool(t)

	c1, c2 := net.Pipe()
	_ = c2.Close() // 对端立即断开 → c1.Write 报错

	p.handleSOCKS5Connect(c1, &ProxyConfig{}, "example.com:443")
	_ = c1.Close()

	p.pairWarmer.mu.RLock()
	pair := p.pairWarmer.primary
	p.pairWarmer.mu.RUnlock()
	if pair == nil {
		t.Fatal("primary pair should exist")
	}
	if refs := atomic.LoadInt32(&pair.refs); refs != 0 {
		t.Fatalf("pair refs = %d after reply-write-fail early return, want 0 (leaked)", refs)
	}
}
