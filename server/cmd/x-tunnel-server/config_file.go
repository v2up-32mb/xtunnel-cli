package main

import (
	"encoding/json"
	"flag"
	"os"
)

type serverFileConfig struct {
	Listen               *string `json:"listen"`
	Token                *string `json:"token"`
	CertFile             *string `json:"cert_file"`
	KeyFile              *string `json:"key_file"`
	MaxTotalChannels     *int    `json:"max_total_channels"`
	MaxChannelsPerClient *int    `json:"max_channels_per_client"`
}

func visitedFlags() map[string]bool {
	provided := make(map[string]bool)
	flag.CommandLine.Visit(func(f *flag.Flag) {
		provided[f.Name] = true
	})
	return provided
}

func applyServerFileConfig(path string, provided map[string]bool) error {
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var cfg serverFileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}

	if cfg.Listen != nil && !provided["l"] {
		listenAddr = *cfg.Listen
	}
	if cfg.Token != nil && !provided["token"] {
		token = *cfg.Token
	}
	if cfg.CertFile != nil && !provided["cert"] {
		certFile = *cfg.CertFile
	}
	if cfg.KeyFile != nil && !provided["key"] {
		keyFile = *cfg.KeyFile
	}
	if cfg.MaxTotalChannels != nil && !provided["max-total-channels"] {
		maxTotalChannels = *cfg.MaxTotalChannels
	}
	if cfg.MaxChannelsPerClient != nil && !provided["max-client-channels"] {
		maxChannelsPerClient = *cfg.MaxChannelsPerClient
	}

	return nil
}
