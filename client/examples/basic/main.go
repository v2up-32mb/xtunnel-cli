package main

import (
	"log"

	"x-tunnel/client/pkg"
)

func main() {
	cfg := &client.Config{
		ServerAddr:  "wss://server:8443",
		Token:       "your_token",
		Connections: 3,
	}

	c, err := client.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
	defer c.Shutdown()

	if err := c.ListenSOCKS5("127.0.0.1:1080"); err != nil {
		log.Fatal(err)
	}

	log.Println("客户端已启动,SOCKS5 代理监听 127.0.0.1:1080")
	select {}
}
