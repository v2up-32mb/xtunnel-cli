package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyClientFileConfigUsesFileWhenFlagNotProvided(t *testing.T) {
	resetClientFlagGlobals()
	path := writeClientConfigTestFile(t, `{"listen":"socks5://127.0.0.1:1080","forward":"wss://example.com","token":"from-file","connections":5,"max_socks5_connections":88,"connect_timeout":"2s"}`)

	if err := applyClientFileConfig(path, map[string]bool{}); err != nil {
		t.Fatalf("apply config failed: %v", err)
	}
	if listenAddr != "socks5://127.0.0.1:1080" || forwardAddr != "wss://example.com" || token != "from-file" {
		t.Fatal("expected config file values to populate client flags")
	}
	if connectionNum != 5 || maxSOCKS5Connections != 88 || connectTimeout != 2*time.Second {
		t.Fatal("expected numeric client config values to populate globals")
	}
}

func TestApplyClientFileConfigKeepsCLIOverrides(t *testing.T) {
	resetClientFlagGlobals()
	listenAddr = "http://127.0.0.1:8080"
	connectionNum = 9
	path := writeClientConfigTestFile(t, `{"listen":"socks5://127.0.0.1:1080","connections":5}`)

	if err := applyClientFileConfig(path, map[string]bool{"l": true, "n": true}); err != nil {
		t.Fatalf("apply config failed: %v", err)
	}
	if listenAddr != "http://127.0.0.1:8080" || connectionNum != 9 {
		t.Fatal("expected CLI-provided values to override config file")
	}
}

func TestParseListenAddrsAcceptsHTTPAndSOCKS5(t *testing.T) {
	resetClientFlagGlobals()
	listenAddr = "socks5://127.0.0.1:1080,http://127.0.0.1:8080"
	listeners := parseListenAddrs()
	if len(listeners) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(listeners))
	}
}

func writeClientConfigTestFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "client-config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}
	return path
}

func resetClientFlagGlobals() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	configFile = ""
	listenAddr = ""
	forwardAddr = ""
	ipAddr = ""
	udpBlockPortsStr = "443"
	token = ""
	insecure = false
	connectionNum = 3
	maxSOCKS5Connections = 1024
	connectTimeout = 15 * time.Second
	ips = ""
	registerFlags(flag.CommandLine)
}
