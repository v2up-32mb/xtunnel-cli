package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type clientFileConfig struct {
	Listen               *string `json:"listen"`
	Forward              *string `json:"forward"`
	IP                   *string `json:"ip"`
	Block                *string `json:"block"`
	Insecure             *bool   `json:"insecure"`
	Token                *string `json:"token"`
	DNS                  *string `json:"dns"`
	ECH                  *string `json:"ech"`
	Fallback             *bool   `json:"fallback"`
	Connections          *int    `json:"connections"`
	MaxSOCKS5Connections *int    `json:"max_socks5_connections"`
	ConnectTimeout       *string `json:"connect_timeout"`
	IPs                  *string `json:"ips"`
}

func visitedFlags() map[string]bool {
	provided := make(map[string]bool)
	flag.CommandLine.Visit(func(f *flag.Flag) {
		provided[f.Name] = true
	})
	return provided
}

func applyClientFileConfig(path string, provided map[string]bool) error {
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var cfg clientFileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}

	if cfg.Listen != nil && !provided["l"] {
		listenAddr = *cfg.Listen
	}
	if cfg.Forward != nil && !provided["f"] {
		forwardAddr = *cfg.Forward
	}
	if cfg.IP != nil && !provided["ip"] {
		ipAddr = *cfg.IP
	}
	if cfg.Block != nil && !provided["block"] {
		udpBlockPortsStr = *cfg.Block
	}
	if cfg.Insecure != nil && !provided["insecure"] {
		insecure = *cfg.Insecure
	}
	if cfg.Token != nil && !provided["token"] {
		token = *cfg.Token
	}
	if cfg.DNS != nil && !provided["dns"] {
		dnsServer = *cfg.DNS
	}
	if cfg.ECH != nil && !provided["ech"] {
		echDomain = *cfg.ECH
	}
	if cfg.Fallback != nil && !provided["fallback"] {
		fallback = *cfg.Fallback
	}
	if cfg.Connections != nil && !provided["n"] {
		connectionNum = *cfg.Connections
	}
	if cfg.MaxSOCKS5Connections != nil && !provided["max-socks5-conns"] {
		maxSOCKS5Connections = *cfg.MaxSOCKS5Connections
	}
	if cfg.IPs != nil && !provided["ips"] {
		ips = *cfg.IPs
	}
	if cfg.ConnectTimeout != nil && !provided["connect-timeout"] {
		d, err := time.ParseDuration(*cfg.ConnectTimeout)
		if err != nil {
			return fmt.Errorf("connect_timeout 无效: %w", err)
		}
		connectTimeout = d
	}

	return nil
}
