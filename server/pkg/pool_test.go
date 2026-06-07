package server

import "testing"

func TestHandleTCPConnectUsesRemoteAddrForClientAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Token = "token"
	p := newServerPool(cfg.Token, cfg)

	p.wsConns = []*ServerWSConn{{
		clientID:   "client-a",
		remoteAddr: "203.0.113.10:4567",
		closed:     true,
	}}
	p.chConns[1] = p.wsConns[0]

	meta := append([]byte{0}, []byte("203.0.113.1:65000")...)
	p.handleTCPConnect(1, "conn-1", meta)

	p.mu.RLock()
	st := p.conns["conn-1"]
	p.mu.RUnlock()
	if st == nil {
		t.Fatalf("connection state was not created")
	}
	if st.clientID != "client-a" {
		t.Fatalf("clientID = %q, want %q", st.clientID, "client-a")
	}
	if st.clientAddr != "203.0.113.10:4567" {
		t.Fatalf("clientAddr = %q, want remote address", st.clientAddr)
	}
}
