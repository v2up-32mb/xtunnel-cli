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
	registerFlags(flag.CommandLine)
}

func registerFlags(fs *flag.FlagSet) {
	fs.StringVar(&configFile, "config", "", "JSON 配置文件路径（可选，CLI 参数优先级更高）")
	fs.StringVar(&listenAddr, "l", ":8443", "监听地址")
	fs.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	fs.StringVar(&certFile, "cert", "", "TLS 证书文件 (不指定则自动生成自签证书)")
	fs.StringVar(&keyFile, "key", "", "TLS 私钥文件 (不指定则自动生成自签证书)")
	fs.IntVar(&maxTotalChannels, "max-total-channels", 0, "服务端最大总通道数，0 表示无限制")
	fs.IntVar(&maxChannelsPerClient, "max-client-channels", 0, "每个客户端最大通道数，0 表示无限制")
	fs.IntVar(&backpressureLimitBytes, "backpressure-limit", 32<<20, "全局队列背压阈值（字节），默认 32MB")
	fs.IntVar(&maxReverseListeners, "max-reverse-listeners", 3, "每个客户端最大反向监听器数，默认 3")
	fs.BoolVar(&hotPair, "hotpair", false, "启用反向预热通道对（Hot Pair，降低反向拨号首帧延迟）")
	fs.IntVar(&hotPairCount, "hotpair-count", 1, "每客户端预热 Pair 数量，默认 1")
	fs.DurationVar(&hotPairRefreshInterval, "hotpair-refresh", 30*time.Second, "预热刷新间隔，默认 30s")
}

var (
	configFile             string
	listenAddr             string
	token                  string
	certFile               string
	keyFile                string
	maxTotalChannels       int
	maxChannelsPerClient   int
	backpressureLimitBytes int
	maxReverseListeners    int
	hotPair                bool
	hotPairCount           int
	hotPairRefreshInterval time.Duration
)

func parseFlags() *server.Config {
	if err := applyServerFileConfig(configFile, visitedFlags()); err != nil {
		log.Fatalf("[服务端] 读取配置文件失败: %v", err)
	}
	if token == "" {
		log.Fatalf("[服务端] 错误: 必须指定 -token 参数")
	}

	// 确定是否使用自签证书；cert 与 key 必须成对提供，否则报错
	autoCert := true
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			log.Fatalf("[服务端] 错误: -cert 与 -key 必须同时提供")
		}
		autoCert = false
	}

	cfg := server.DefaultConfig()
	cfg.ListenAddr = listenAddr
	cfg.Token = token
	cfg.CertFile = certFile
	cfg.KeyFile = keyFile
	cfg.AutoCert = autoCert
	cfg.MaxTotalChannels = maxTotalChannels
	cfg.MaxChannelsPerClient = maxChannelsPerClient
	cfg.BackpressureLimitBytes = backpressureLimitBytes
	cfg.MaxReverseListeners = maxReverseListeners
	cfg.EnableHotPair = hotPair
	cfg.HotPairCount = hotPairCount
	cfg.HotPairRefreshInterval = hotPairRefreshInterval

	return cfg
}
