package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientPoolStartWaitsForECHPreparationBeforeReturning(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ServerAddr = "wss://127.0.0.1:1"
	cfg.EnableECH = true
	cfg.DNSServer = "127.0.0.1:1"
	cfg.ECHDomain = "invalid.example"
	cfg.Connections = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		p.Start(nil)
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("Start() returned before initial ECH preparation completed")
	case <-time.After(150 * time.Millisecond):
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Start() did not return after context cancellation")
	}
}

func TestDialWebSocketIncludesStableClientID(t *testing.T) {
	type requestInfo struct {
		clientID string
		chID     string
	}

	requests := make(chan requestInfo, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- requestInfo{
			clientID: r.URL.Query().Get("client_id"),
			chID:     r.URL.Query().Get("ch_id"),
		}
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.ServerAddr = strings.Replace(server.URL, "https://", "wss://", 1)
	cfg.EnableECH = false
	cfg.InsecureSkipVerify = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	conn1, err := p.dialWebSocket(1, "")
	if err != nil {
		t.Fatalf("first dialWebSocket() error = %v", err)
	}
	_ = conn1.Close()

	conn2, err := p.dialWebSocket(2, "")
	if err != nil {
		t.Fatalf("second dialWebSocket() error = %v", err)
	}
	_ = conn2.Close()

	first := <-requests
	second := <-requests

	if first.clientID == "" {
		t.Fatalf("first request missing client_id")
	}
	if second.clientID == "" {
		t.Fatalf("second request missing client_id")
	}
	if first.clientID != second.clientID {
		t.Fatalf("client_id should stay stable across dials: first=%q second=%q", first.clientID, second.clientID)
	}
	if first.chID != "1" {
		t.Fatalf("first ch_id = %q, want 1", first.chID)
	}
	if second.chID != "2" {
		t.Fatalf("second ch_id = %q, want 2", second.chID)
	}
}

func TestDialWebSocketReturnsContextErrorQuicklyWhenCancelledDuringECHRetryWait(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ServerAddr = "wss://example.com:443"
	cfg.EnableECH = true
	cfg.DNSServer = "127.0.0.1:1"
	cfg.ECHDomain = "invalid.example"

	ctx, cancel := context.WithCancel(context.Background())
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	cancel()
	start := time.Now()
	_, err = p.dialWebSocket(1, "")
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dialWebSocket() error = %v, want context.Canceled", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("dialWebSocket() returned too slowly after cancellation: %v", elapsed)
	}
}

func TestDialAndServeStopsPromptlyWhenCancelledDuringReconnectDelay(t *testing.T) {
	connected := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		select {
		case connected <- struct{}{}:
		default:
		}
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = conn.Close()
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.ServerAddr = strings.Replace(server.URL, "https://", "wss://", 1)
	cfg.EnableECH = false
	cfg.InsecureSkipVerify = true
	cfg.ReconnectDelay = time.Second
	cfg.Connections = 1

	ctx, cancel := context.WithCancel(context.Background())
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		p.dialAndServe(0, "")
		close(done)
	}()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatalf("dialAndServe() did not establish initial connection")
	}

	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
			t.Fatalf("dialAndServe() returned too slowly after cancellation: %v", elapsed)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatalf("dialAndServe() did not stop promptly during reconnect delay")
	}
}

func TestDialAndServeKeepsRetryingAfterRetryLimit(t *testing.T) {
	oldBaseDelay := dialAndServeBaseDelay
	oldMaxDelay := dialAndServeMaxDelay
	oldMaxRetries := dialAndServeMaxRetries
	defer func() {
		dialAndServeBaseDelay = oldBaseDelay
		dialAndServeMaxDelay = oldMaxDelay
		dialAndServeMaxRetries = oldMaxRetries
	}()

	dialAndServeBaseDelay = time.Millisecond
	dialAndServeMaxDelay = 2 * time.Millisecond
	dialAndServeMaxRetries = 2

	cfg := DefaultConfig()
	cfg.ServerAddr = "wss://127.0.0.1:1"
	cfg.EnableECH = false
	cfg.DialTimeout = 10 * time.Millisecond
	cfg.Connections = 1

	ctx, cancel := context.WithCancel(context.Background())
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		p.dialAndServe(0, "")
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("dialAndServe() returned after hitting retry limit, want it to keep retrying")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("dialAndServe() did not exit after cancellation")
	}
}
