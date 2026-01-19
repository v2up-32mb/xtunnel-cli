// Package client 提供基于 WebSocket 的隧道代理客户端。
//
// 基本使用:
//
//	cfg := &client.Config{
//	    ServerAddr: "wss://server:8443",
//	    Token:      "your_token",
//	}
//	c, err := client.NewClient(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := c.Start(); err != nil {
//	    log.Fatal(err)
//	}
//	defer c.Shutdown()
//
//	// 启动 SOCKS5 代理
//	go c.ListenSOCKS5("127.0.0.1:1080")
//	select {}
package client
