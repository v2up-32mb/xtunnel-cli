package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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
	cfg := DefaultConfig()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewECHManager(cfg, parent)

	expected := errors.New("udp failed")
	m.queryDNSUDPFn = func(domain, dnsServer string) (string, error) {
		return "", expected
	}

	_, err := m.queryHTTPSRecord("example.com", "8.8.8.8:53")
	if !errors.Is(err, expected) {
		t.Fatalf("queryHTTPSRecord() error = %v, want %v", err, expected)
	}
}
