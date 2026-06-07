package client

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"x-tunnel/common"
)

func TestReserveQueueBytesRejectsWithoutChangingTotal(t *testing.T) {
	p := &clientPool{globalQueueLimit: 10}
	atomic.StoreInt64(&p.globalQueueBytes, 9)

	if p.reserveQueueBytes(2) {
		t.Fatal("expected reserveQueueBytes to reject over-limit reservation")
	}
	if got := atomic.LoadInt64(&p.globalQueueBytes); got != 9 {
		t.Fatalf("expected queue bytes to stay 9, got %d", got)
	}
}

func TestReleaseQueueBytesClampsAtZero(t *testing.T) {
	p := &clientPool{}
	p.releaseQueueBytes(5)

	if got := atomic.LoadInt64(&p.globalQueueBytes); got != 0 {
		t.Fatalf("expected queue bytes to clamp at 0, got %d", got)
	}
}

func TestBroadcastWriteWithoutActiveConnectionsDoesNotEnqueue(t *testing.T) {
	p := &clientPool{
		writeQueues:      []chan writeJob{make(chan writeJob, 1)},
		wsConns:          []*websocket.Conn{nil},
		globalQueueLimit: 1024,
		nextChannel:      1,
	}

	p.broadcastWrite(websocket.BinaryMessage, []byte("hello"))

	if got := len(p.writeQueues[0]); got != 0 {
		t.Fatalf("expected no queued messages without active connections, got %d", got)
	}
	if got := atomic.LoadInt64(&p.globalQueueBytes); got != 0 {
		t.Fatalf("expected queue bytes to stay 0, got %d", got)
	}
}

func TestWriteWorkerAggregatedJobsReleaseAllQueueBytes(t *testing.T) {
	conn, recvCh, cleanup := newTestWebSocketConn(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &clientPool{
		ctx:             ctx,
		config:          &Config{PingInterval: time.Hour, WriteTimeout: time.Second, ReadBufferSize: 1024},
		connsWriteMutex: []sync.Mutex{{}},
	}

	queue := make(chan writeJob, 4)
	msg1 := common.EncodeMessage(common.MsgTCPData, "cid", nil, []byte("hello"))
	msg2 := common.EncodeMessage(common.MsgTCPData, "cid", nil, []byte("world"))
	queue <- writeJob{msgType: websocket.BinaryMessage, data: msg1, size: len(msg1)}
	queue <- writeJob{msgType: websocket.BinaryMessage, data: msg2, size: len(msg2)}
	close(queue)
	atomic.StoreInt64(&p.globalQueueBytes, int64(len(msg1)+len(msg2)))

	done := make(chan struct{})
	go func() {
		p.writeWorker(0, conn, queue)
		close(done)
	}()

	select {
	case <-recvCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for aggregated websocket message")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeWorker did not exit")
	}

	if got := atomic.LoadInt64(&p.globalQueueBytes); got != 0 {
		t.Fatalf("expected queue bytes to be fully released, got %d", got)
	}
}

func TestUnregisterUDPAssociationReturnsPromptly(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &clientPool{
		ctx:    ctx,
		cancel: cancel,
		config: DefaultConfig(),
		conns:  make(map[string]*clientConnState),
	}

	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatalf("listen udp failed: %v", err)
	}
	defer listener.Close()

	assoc := &udpAssociation{
		connID:      "udp-conn",
		tcpConn:     serverConn,
		udpListener: listener,
		pool:        p,
		done:        make(chan bool, 1),
	}
	p.conns[assoc.connID] = &clientConnState{
		udpAssoc: assoc,
		reqType:  "SOCKS5 UDP",
	}

	done := make(chan struct{})
	go func() {
		p.Unregister(assoc.connID)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected Unregister to return promptly for UDP association")
	}
}

func TestClientStatsCountSentBytesFromWriteWorker(t *testing.T) {
	conn, recvCh, cleanup := newTestWebSocketConn(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &clientPool{
		ctx:             ctx,
		config:          &Config{PingInterval: time.Hour, WriteTimeout: time.Second, ReadBufferSize: 1024},
		relayManager:    NewRelayNodeManager(),
		connsWriteMutex: []sync.Mutex{{}},
	}

	msg := common.EncodeMessage(common.MsgConnStatus, "cid", []byte{byte(common.StatusOK)}, nil)
	queue := make(chan writeJob, 1)
	queue <- writeJob{msgType: websocket.BinaryMessage, data: msg, size: len(msg)}
	close(queue)

	done := make(chan struct{})
	go func() {
		p.writeWorker(0, conn, queue)
		close(done)
	}()

	select {
	case <-recvCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for websocket message to be sent")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writeWorker did not exit")
	}

	if got := p.Stats().BytesSent; got != uint64(len(msg)) {
		t.Fatalf("expected bytes sent %d, got %d", len(msg), got)
	}
}

func TestClientStatsCountReceivedBytesFromHandleChannel(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &clientPool{
		ctx:          ctx,
		cancel:       cancel,
		config:       &Config{ReadTimeout: time.Second, WriteTimeout: time.Second},
		relayManager: NewRelayNodeManager(),
		conns:        make(map[string]*clientConnState),
	}

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()

	msg := common.EncodeMessage(common.MsgConnStatus, "cid", []byte{byte(common.StatusOK)}, nil)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
		t.Fatalf("write message failed: %v", err)
	}
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleChannel did not exit")
	}

	if got := p.Stats().BytesReceived; got != uint64(len(msg)) {
		t.Fatalf("expected bytes received %d, got %d", len(msg), got)
	}
}

func newTestWebSocketConn(t *testing.T) (*websocket.Conn, <-chan []byte, func()) {
	t.Helper()

	recvCh := make(chan []byte, 1)
	serverDone := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			close(serverDone)
			return
		}
		defer ws.Close()
		defer close(serverDone)

		_, msg, err := ws.ReadMessage()
		if err == nil {
			recvCh <- msg
		}
	}))

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial websocket failed: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Close()
		select {
		case <-serverDone:
		case <-time.After(time.Second):
		}
	}

	return conn, recvCh, cleanup
}

func newClientTestWebSocketPair(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
	t.Helper()

	serverConnCh := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	serverDone := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			close(serverDone)
			return
		}
		serverConnCh <- ws
		<-release
		_ = ws.Close()
		close(serverDone)
	}))

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial websocket failed: %v", err)
	}

	var serverConn *websocket.Conn
	select {
	case serverConn = <-serverConnCh:
	case <-time.After(time.Second):
		_ = clientConn.Close()
		server.Close()
		t.Fatal("timed out waiting for server websocket connection")
	}

	cleanup := func() {
		close(release)
		_ = clientConn.Close()
		_ = serverConn.Close()
		server.Close()
		select {
		case <-serverDone:
		case <-time.After(time.Second):
		}
	}

	return clientConn, serverConn, cleanup
}
