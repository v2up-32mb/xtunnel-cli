package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"x-tunnel/server/pkg"
)

func main() {
	log.Printf("[服务端] 程序启动")
	flag.Parse()

	cfg := parseFlags()

	s, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("[服务端] 创建服务端失败: %v", err)
	}

	if err := s.Start(); err != nil {
		log.Fatalf("[服务端] 启动服务端失败: %v", err)
	}
	defer s.Shutdown()

	log.Printf("[服务端] 已启动,等待连接...")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("[服务端] 收到退出信号,正在关闭...")
}

func init() {
	flag.StringVar(&listenAddr, "l", ":8443", "监听地址")
	flag.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	flag.StringVar(&certFile, "cert", "", "TLS 证书文件 (不指定则自动生成自签证书)")
	flag.StringVar(&keyFile, "key", "", "TLS 私钥文件 (不指定则自动生成自签证书)")
}

var (
	listenAddr string
	token      string
	certFile   string
	keyFile    string
)

func parseFlags() *server.Config {
	if token == "" {
		log.Fatalf("[服务端] 错误: 必须指定 -token 参数")
	}

	// 确定是否使用自签证书
	autoCert := true
	if certFile != "" && keyFile != "" {
		autoCert = false
	}

	cfg := &server.Config{
		ListenAddr:       listenAddr,
		Token:            token,
		CertFile:         certFile,
		KeyFile:          keyFile,
		AutoCert:         autoCert,
		ReadTimeout:      15 * time.Second,
		WriteTimeout:     5 * time.Second,
		PingInterval:     5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		ReadBufferSize:   64 * 1024,
		WriteBufferSize:  64 * 1024,
	}

	return cfg
}
