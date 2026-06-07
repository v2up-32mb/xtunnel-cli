package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestQueryDoHContextCancelReturnsQuickly 验证 DoH 查询应能随 context 取消快速返回。
// 当前实现若未把 context 绑定到 HTTP 请求，这个测试会先失败。
func TestQueryDoHContextCancelReturnsQuickly(t *testing.T) {
	hangHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	server := httptest.NewServer(hangHandler)
	defer server.Close()

	cfg := &Config{
		EnableECH: true,
		ECHDomain: "example.com",
		DNSServer: server.URL,
	}
	m := NewECHManager(cfg, context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := m.queryHTTPSRecord(cfg.ECHDomain, cfg.DNSServer)
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	m.cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("queryDoH should return quickly after cancel, took %v", elapsed)
		}
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("queryDoH did not return promptly after cancel")
	}
}

// TestBuildTLSConfigECHDisabled 锁定 ECH disabled 时应走普通 TLS 配置。
// TestQueryDNSUDPContextCancelReturnsQuickly 验证 UDP DNS 查询也应能随 context 取消快速返回。
// 当前实现若 UDP 读阻塞不能被取消，这个测试会先失败。
func TestQueryDNSUDPContextCancelReturnsQuickly(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create UDP listener: %v", err)
	}
	defer pc.Close()

	cfg := &Config{
		EnableECH: true,
		ECHDomain: "example.com",
		DNSServer: pc.LocalAddr().String(),
	}
	m := NewECHManager(cfg, context.Background())

	// 只读包不响应，制造 Read 阻塞。
	go func() {
		buf := make([]byte, 4096)
		_, _, _ = pc.ReadFrom(buf)
	}()

	done := make(chan error, 1)
	go func() {
		_, err := m.queryHTTPSRecord(cfg.ECHDomain, cfg.DNSServer)
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	m.cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("queryDNSUDP should return quickly after cancel, took %v", elapsed)
		}
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("queryDNSUDP did not return promptly after cancel")
	}
}

// TestPrepareReturnsOnCancel 验证 Prepare 在查询过程中被取消后应及时返回。
func TestPrepareReturnsOnCancel(t *testing.T) {
	hangHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	server := httptest.NewServer(hangHandler)
	defer server.Close()

	cfg := &Config{
		EnableECH: true,
		ECHDomain: "example.com",
		DNSServer: server.URL,
	}
	m := NewECHManager(cfg, context.Background())

	done := make(chan error, 1)
	go func() {
		done <- m.Prepare()
	}()

	time.Sleep(100 * time.Millisecond)
	m.cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Prepare to return error after cancel")
		}
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("Prepare did not return promptly after cancel")
	}
}

// TestStopIdempotent 验证 Stop 可重复调用而不 panic。
func TestStopIdempotent(t *testing.T) {
	cfg := &Config{EnableECH: false}
	m := NewECHManager(cfg, context.Background())
	m.Stop()
	m.Stop()
}

func TestBuildTLSConfigECHDisabled(t *testing.T) {
	cfg := &Config{EnableECH: false}
	m := NewECHManager(cfg, context.Background())

	tlsCfg, err := m.BuildTLSConfig("example.com")
	if err != nil {
		t.Fatalf("BuildTLSConfig returned error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("BuildTLSConfig returned nil config")
	}
	if tlsCfg.EncryptedClientHelloConfigList != nil {
		t.Fatal("expected standard TLS config when ECH is disabled")
	}
	if tlsCfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("unexpected MinVersion: got %x", tlsCfg.MinVersion)
	}
	if tlsCfg.ServerName != "example.com" {
		t.Fatalf("unexpected ServerName: got %q", tlsCfg.ServerName)
	}
}

func TestECHManagerStopsWhenParentContextIsCancelled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableECH = true
	cfg.DNSServer = "127.0.0.1:1"
	cfg.ECHDomain = "invalid.example"

	parent, cancel := context.WithCancel(context.Background())
	m := NewECHManager(cfg, parent)
	done := make(chan error, 1)

	go func() {
		done <- m.Prepare()
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Prepare() returned nil after cancellation, want context error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Prepare() did not return after parent context cancellation")
	}
}

func TestQueryHTTPSRecordFallsBackFromDoHToUDP(t *testing.T) {
	cfg := DefaultConfig()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewECHManager(cfg, parent)

	dohServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer dohServer.Close()

	m.queryDNSUDPFn = func(domain, dnsServer string) (string, error) {
		if dnsServer != "8.8.8.8:53" {
			t.Fatalf("fallback dns server = %q, want %q", dnsServer, "8.8.8.8:53")
		}
		return "ech-value", nil
	}

	got, err := m.queryHTTPSRecord("example.com", dohServer.URL)
	if err != nil {
		t.Fatalf("queryHTTPSRecord() error = %v", err)
	}
	if got != "ech-value" {
		t.Fatalf("queryHTTPSRecord() = %q, want %q", got, "ech-value")
	}
}

func TestQueryHTTPSRecordReturnsOriginalErrorWhenFallbackFails(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewECHManager(DefaultConfig(), parent)

	expected := errors.New("udp failed")
	m.queryDNSUDPFn = func(domain, dnsServer string) (string, error) {
		return "", expected
	}

	_, err := m.queryHTTPSRecord("example.com", "8.8.8.8:53")
	if !errors.Is(err, expected) {
		t.Fatalf("queryHTTPSRecord() error = %v, want %v", err, expected)
	}
}
