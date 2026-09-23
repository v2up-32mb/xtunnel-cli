package server

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// newWarmTestServer 构建启用 Hot Pair 的测试服务端 + 双通道假客户端
func newWarmTestServer(t *testing.T) (*serverPool, *httptest.Server, *websocket.Conn, *websocket.Conn, func()) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Token = "token"
	p := newServerPool(cfg.Token, cfg)
	// 预热器常驻，需模拟客户端 MsgReverseHotPair 授权后才会预热

	server := httptest.NewServer(http.HandlerFunc(p.handleWebSocket))
	conn1 := dialServerWS(t, server, "client-a", 1)
	conn2 := dialServerWS(t, server, "client-a", 2)

	// 等待两个通道注册就绪
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.RLock()
		ch1 := p.clientChConns["client-a"][1]
		ch2 := p.clientChConns["client-a"][2]
		p.mu.RUnlock()
		if ch1 != nil && ch2 != nil && !ch1.closed && !ch2.closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("channels not registered in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cleanup := func() {
		_ = conn1.Close()
		_ = conn2.Close()
		server.Close()
	}
	return p, server, conn1, conn2, cleanup
}

func readFrameType(t *testing.T, conn *websocket.Conn, timeout time.Duration) (protocol.MessageType, string, []byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	mtype, connID, meta, payload, err := protocol.DecodeMessage(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return mtype, connID, meta, payload
}

func be32v(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// TestReversePairWarmerBuildAndAcquire 预热构建 → Ready → 一次性消费 → 通道清理废弃
func TestReversePairWarmerBuildAndAcquire(t *testing.T) {
	p, _, conn1, _, cleanup := newWarmTestServer(t)
	defer cleanup()

	// 注册反向监听（端口 0 → 随机端口），使客户端进入预热名单
	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	// 模拟客户端 MsgReverseHotPair 授权（经 handleMessage 路由）
	p.handleMessage("client-a", 1, 4, protocol.MsgReverseHotPair, "", nil, nil)
	if !p.reversePairWarmer.Enabled("client-a") {
		t.Fatal("client should be enabled after MsgReverseHotPair")
	}
	// 回执帧由假客户端消费
	mtype, _, gotMeta, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype != protocol.MsgReverseListenResult || gotMeta[0] != byte(protocol.StatusOK) {
		t.Fatalf("expected listen result OK, got type=%d meta=%v", mtype, gotMeta)
	}

	// 预热一轮
	p.reversePairWarmer.tryBuildPairs()

	// 假客户端收到 MsgPrebindRequest，回 MsgSelectUplink([1])（客户端选定通道 1）
	mtype, gotID, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype != protocol.MsgPrebindRequest || !isReversePrebindConnID(gotID) {
		t.Fatalf("expected MsgPrebindRequest, got type=%d id=%s", mtype, gotID)
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, gotID, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	// 预热收尾：服务端经 P1 推送 MsgSelectDownlink([P2])，至此上下行选路全部完成
	mtypeD, idD, metaD, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeD != protocol.MsgSelectDownlink || idD != gotID || binary.BigEndian.Uint32(metaD) != 1 {
		t.Fatalf("expected warmup MsgSelectDownlink P2=1, got type=%d id=%s meta=%v", mtypeD, idD, metaD)
	}

	// Pair 就绪：P1=1（meta），P2=1（到达通道）
	deadline := time.Now().Add(2 * time.Second)
	var pair *ReverseHotPair
	for time.Now().Before(deadline) {
		if pair = p.reversePairWarmer.AcquirePair("client-a"); pair != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pair == nil {
		t.Fatal("pair never became ready")
	}
	if pair.P1 != 1 || pair.P2 != 1 {
		t.Fatalf("expected P1=1 P2=1, got P1=%d P2=%d", pair.P1, pair.P2)
	}
	if again := p.reversePairWarmer.AcquirePair("client-a"); again != nil {
		t.Fatal("pair should be consumed once")
	}

	// 重建一个 Pair 后模拟通道断开 → 废弃
	p.reversePairWarmer.tryBuildPairs()
	mtype, gotID, _, _ = readFrameType(t, conn1, 3*time.Second)
	if mtype != protocol.MsgPrebindRequest {
		t.Fatalf("expected second MsgPrebindRequest, got %d", mtype)
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, gotID, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	readFrameType(t, conn1, 3*time.Second) // 消费预热收尾 MsgSelectDownlink
	deadline = time.Now().Add(2 * time.Second)
	for {
		p.reversePairWarmer.mu.Lock()
		n := len(p.reversePairWarmer.ready["client-a"])
		p.reversePairWarmer.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second pair never ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.cleanupChannel("client-a", 1)
	if pair := p.reversePairWarmer.AcquirePair("client-a"); pair != nil {
		t.Fatal("pair should be invalidated after channel cleanup")
	}
}

// TestReversePairWarmerClientGoneRevokes 客户端全部通道掉线 → 授权撤销
func TestReversePairWarmerClientGoneRevokes(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	p.handleMessage("client-a", 1, 4, protocol.MsgReverseHotPair, "", nil, nil)
	if !p.reversePairWarmer.Enabled("client-a") {
		t.Fatal("client should be enabled")
	}
	// 关闭该客户端全部通道 → 最后一条清理时 clientGone → 撤销授权
	_ = conn1.Close()
	_ = conn2.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !p.reversePairWarmer.Enabled("client-a") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("authorization should be revoked when client is gone")
}

// TestReversePairWarmerNotEnabledNoWarm 未授权客户端不预热
func TestReversePairWarmerNotEnabledNoWarm(t *testing.T) {
	p, _, conn1, _, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	readFrameType(t, conn1, 3*time.Second) // 消费监听回执
	p.reversePairWarmer.tryBuildPairs()

	_ = conn1.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := conn1.ReadMessage(); err == nil {
		t.Fatal("unauthorized client should not receive prebind request")
	}
}

// TestReverseDialStreamHotPairUnicast 就绪 Pair 下 DialStream 单播直达，不广播
func TestReverseDialStreamHotPairUnicast(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	readFrameType(t, conn1, 3*time.Second) // 消费监听回执
	p.handleMessage("client-a", 1, 4, protocol.MsgReverseHotPair, "", nil, nil)

	// 假目标服务（客户端侧出口）
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := targetLn.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	// 预热构建 Pair（通道 1）；PrebindRequest 是广播，两个通道都会收到
	p.reversePairWarmer.tryBuildPairs()
	mtype, gotID, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype != protocol.MsgPrebindRequest {
		t.Fatalf("expected MsgPrebindRequest, got %d", mtype)
	}
	readFrameType(t, conn2, 3*time.Second) // 消费 ch2 的广播副本
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, gotID, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	mtypeP, idP, metaP, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeP != protocol.MsgSelectDownlink || idP != gotID || binary.BigEndian.Uint32(metaP) != 1 {
		t.Fatalf("expected warmup MsgSelectDownlink P2=1, got type=%d id=%s meta=%v", mtypeP, idP, metaP)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if pair := p.reversePairWarmer.AcquirePair("client-a"); pair != nil {
			// 释放回队列供 DialStream 取用：直接塞回
			p.reversePairWarmer.mu.Lock()
			p.reversePairWarmer.ready["client-a"] = append(p.reversePairWarmer.ready["client-a"], pair)
			p.reversePairWarmer.mu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pair never ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 通道 2 不回 SelectUplink——DialStream 不得向其广播
	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), targetLn.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	// 通道 1 收到单播 MsgTCPConnect（复用 prebind connID；拨号期零选路消息）
	mtype1, id1, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect || !isReversePrebindConnID(id1) || id1 != gotID {
		t.Fatalf("expected MsgTCPConnect with pair connID, got type=%d id=%s (pair=%s)", mtype1, id1, gotID)
	}

	// 通道 2 在 PrebindRequest 之后不应收到任何消息（单播而非广播）
	_ = conn2.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := conn2.ReadMessage(); err == nil {
		t.Fatal("channel 2 should not receive unicast dial request")
	}

	// 假客户端回 OK → DialStream 返回
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}
	var stream net.Conn
	select {
	case stream = <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}

	// 数据面（由测试模拟客户端）：上行 stream→隧道→通道1，下行通道1→隧道→stream
	if _, err := stream.Write([]byte("PING")); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	mtypeU, _, _, payloadU := readFrameType(t, conn1, 3*time.Second)
	if mtypeU != protocol.MsgTCPData || string(payloadU) != "PING" {
		t.Fatalf("expected upstream MsgTCPData PING, got type=%d payload=%q", mtypeU, string(payloadU))
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, id1, nil, []byte("PONG"))); err != nil {
		t.Fatalf("write downstream: %v", err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf2 := make([]byte, 16)
	n2, err := stream.Read(buf2)
	if err != nil || string(buf2[:n2]) != "PONG" {
		t.Fatalf("stream got %q err=%v, want PONG", string(buf2[:max(n2, 0)]), err)
	}
}

// TestReverseDialStreamFallback 无就绪 Pair 时回退广播
func TestReverseDialStreamFallback(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := targetLn.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), targetLn.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	// 广播：两个通道都应收到 MsgTCPConnect
	mtype1, id1, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect || isReversePrebindConnID(id1) {
		t.Fatalf("expected broadcast MsgTCPConnect on ch1, got type=%d id=%s", mtype1, id1)
	}
	mtype2, id2, _, _ := readFrameType(t, conn2, 3*time.Second)
	if mtype2 != protocol.MsgTCPConnect || id2 != id1 {
		t.Fatalf("expected same MsgTCPConnect on ch2, got type=%d id=%s", mtype2, id2)
	}

	// 通道 1 竞争获胜：回 MsgSelectUplink；服务端应回 MsgSelectDownlink
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, id1, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	mtypeS, _, metaS, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeS != protocol.MsgSelectDownlink || binary.BigEndian.Uint32(metaS) != 1 {
		t.Fatalf("expected MsgSelectDownlink, got type=%d", mtypeS)
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}

	select {
	case <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}
	_ = accepted
}

// TestReversePairWarmerFallbackRepair 客户端 warm 状态丢失后重新选路：
// 服务端采用客户端的收包通道作为发包通道，并幂等补发 MsgSelectDownlink 修复。
func TestReversePairWarmerFallbackRepair(t *testing.T) {
	p, _, conn1, _, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	readFrameType(t, conn1, 3*time.Second) // 消费监听回执
	p.handleMessage("client-a", 1, 4, protocol.MsgReverseHotPair, "", nil, nil)

	p.reversePairWarmer.tryBuildPairs()
	mtype, gotID, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype != protocol.MsgPrebindRequest {
		t.Fatalf("expected MsgPrebindRequest, got %d", mtype)
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, gotID, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	mtypeP, _, metaP, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeP != protocol.MsgSelectDownlink || binary.BigEndian.Uint32(metaP) != 1 {
		t.Fatalf("expected warmup MsgSelectDownlink, got type=%d", mtypeP)
	}
	// 等待 Pair 入队
	deadline := time.Now().Add(2 * time.Second)
	for {
		if pair := p.reversePairWarmer.AcquirePair("client-a"); pair != nil {
			p.reversePairWarmer.mu.Lock()
			p.reversePairWarmer.ready["client-a"] = append(p.reversePairWarmer.ready["client-a"], pair)
			p.reversePairWarmer.mu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pair never ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), "example.test:443")
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	// 拨号期：只有 MsgTCPConnect（复用 prebind connID），零选路消息
	mtype1, id1, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect || !isReversePrebindConnID(id1) || id1 != gotID {
		t.Fatalf("expected MsgTCPConnect with pair connID, got type=%d id=%s", mtype1, id1)
	}

	// 模拟客户端丢失 warm 状态：回退广播路径行为——回 MsgSelectUplink([1])
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, id1, be32v(1), nil)); err != nil {
		t.Fatalf("write late MsgSelectUplink: %v", err)
	}
	// 服务端应补发一次 MsgSelectDownlink([P2]=1)（幂等修复，且不产生第二份）
	mtypeR, idR, metaR, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeR != protocol.MsgSelectDownlink || idR != id1 || binary.BigEndian.Uint32(metaR) != 1 {
		t.Fatalf("expected repair MsgSelectDownlink, got type=%d id=%s meta=%v", mtypeR, idR, metaR)
	}

	// 拨号成功收尾
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}
	select {
	case <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}
}
