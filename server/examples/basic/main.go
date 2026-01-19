//go:build server
// +build server

package main

import (
	"log"
	"time"

	"x-tunnel/server/pkg"
)

func main() {
	cfg := &server.Config{
		ListenAddr:       ":8443",
		Token:            "your_token",
		AutoCert:         true,
		ReadTimeout:      15 * time.Second,
		WriteTimeout:     5 * time.Second,
		PingInterval:     5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		ReadBufferSize:   64 * 1024,
		WriteBufferSize:  64 * 1024,
	}

	s, err := server.NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
	defer s.Shutdown()

	log.Println("服务端已启动，监听 :8443")
	select {}
}
