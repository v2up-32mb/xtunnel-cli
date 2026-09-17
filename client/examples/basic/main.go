package main

import (
	"log"

	"github.com/v2up-32mb/xtunnel"
	xsharedconfig "github.com/v2up-32mb/xshared/config"
	xsharedsocks5 "github.com/v2up-32mb/xshared/socks5"
)

func main() {
	cfg := &xtunnel.Config{
		ServerAddr:  "wss://server:8443",
		Token:       "your_token",
		Connections: 3,
	}

	c, err := xtunnel.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
	defer c.Shutdown()

	if err := xsharedsocks5.NewServer(
		&xsharedconfig.Config{ListenAddress: "127.0.0.1:1080"},
		c.ProxyDialer(),
	).Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("客户端已启动,SOCKS5 代理监听 127.0.0.1:1080")
	select {}
}
