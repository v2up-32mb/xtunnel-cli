package client

import (
	"context"
	"net"
	"testing"
	"time"
)

type testUDPConn struct {
	closeCalls int
}

func TestUDPAssociationCloseIsIdempotent(t *testing.T) {
	cfg := DefaultConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	listener, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}
	defer listener.Close()

	a := &udpAssociation{
		connID:      "conn-1",
		udpListener: listener,
		pool:        p,
		done:        make(chan bool, 5),
		channelID:   -1,
	}

	a.Close()
	a.Close()
}

func TestUDPAssociationCloseStopsBlockedLoop(t *testing.T) {
	cfg := DefaultConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool() error = %v", err)
	}

	listener, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}

	a := &udpAssociation{
		connID:      "conn-2",
		udpListener: listener,
		pool:        p,
		done:        make(chan bool, 5),
		channelID:   -1,
	}

	done := make(chan struct{})
	go func() {
		a.loop()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	a.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("udpAssociation.loop() did not exit after Close()")
	}
}
