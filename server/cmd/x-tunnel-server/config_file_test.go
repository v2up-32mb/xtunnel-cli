package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestApplyServerFileConfigUsesFileWhenFlagNotProvided(t *testing.T) {
	resetServerFlagGlobals()
	path := writeServerConfigTestFile(t, `{"listen":":9443","token":"from-file","max_total_channels":10,"max_channels_per_client":3}`)

	if err := applyServerFileConfig(path, map[string]bool{}); err != nil {
		t.Fatalf("apply config failed: %v", err)
	}
	if listenAddr != ":9443" || token != "from-file" {
		t.Fatal("expected config file values to populate server flags")
	}
	if maxTotalChannels != 10 || maxChannelsPerClient != 3 {
		t.Fatal("expected server channel limits from config file")
	}
}

func TestApplyServerFileConfigKeepsCLIOverrides(t *testing.T) {
	resetServerFlagGlobals()
	listenAddr = ":8443"
	maxTotalChannels = 7
	path := writeServerConfigTestFile(t, `{"listen":":9443","max_total_channels":10}`)

	if err := applyServerFileConfig(path, map[string]bool{"l": true, "max-total-channels": true}); err != nil {
		t.Fatalf("apply config failed: %v", err)
	}
	if listenAddr != ":8443" || maxTotalChannels != 7 {
		t.Fatal("expected CLI-provided server values to override config file")
	}
}

func writeServerConfigTestFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server-config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}
	return path
}

func resetServerFlagGlobals() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	configFile = ""
	listenAddr = ":8443"
	token = ""
	certFile = ""
	keyFile = ""
	maxTotalChannels = 0
	maxChannelsPerClient = 0
	registerFlags(flag.CommandLine)
}
