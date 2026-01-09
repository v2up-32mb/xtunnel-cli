//go:build client

package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
)

var (
	listenAddr       string        // 监听地址
	forwardAddr      string        // 服务端转发地址
	ipAddr           string        // 目标 IP 地址（可选）
	udpBlockPortsStr string        // UDP 拦截端口列表
	token            string        // 身份验证令牌
	insecure         bool          // 跳过证书验证
	connectionNum    int           // 每个 IP 的连接数
	ips              string        // IP 策略

	clientPool    *ClientPool      // 客户端连接池
	clientID      string           // 客户端唯一标识
	udpBlockPorts map[int]struct{} // UDP 拦截端口集合
)

// init 注册命令行参数
func init() {
	flag.StringVar(&listenAddr, "l", "", "监听地址 (仅支持 socks5://，支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080")
	flag.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式，必须是 wss://host:port/path)")
	flag.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（将 wss 主机名定向到该 IP 连接），多个IP用逗号分隔")
	flag.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表，逗号分隔，如 443,8443")
	flag.BoolVar(&insecure, "insecure", false, "wss 模式忽略证书校验")
	flag.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	flag.IntVar(&connectionNum, "n", 3, "每个IP建立的WebSocket连接数量")
	flag.StringVar(&ips, "ips", "", "服务端解析目标地址的IP偏好\n 4: 仅IPv4\n 6: 仅IPv6\n 4,6: IPv4优先\n 6,4: IPv6优先")
}

// main 客户端入口函数
//
// 初始化客户端并启动服务：
// 1. 解析命令行参数
// 2. 验证配置
// 3. 创建连接池
// 4. 启动 SOCKS5 监听器
// 5. 等待退出信号
func main() {
	flag.Parse()

	if listenAddr == "" || forwardAddr == "" {
		flag.Usage()
		return
	}

	// 仅支持 socks5:// 监听
	listeners := strings.Split(listenAddr, ",")
	for _, l := range listeners {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if !strings.HasPrefix(l, "socks5://") {
			log.Fatalf("[客户端] 已移除除 SOCKS5 外的监听支持：非法监听地址 %q", l)
		}
	}

	// 验证服务端地址
	forwardURL, err := url.Parse(forwardAddr)
	if err != nil {
		log.Fatalf("[客户端] 无效的服务地址: %v", err)
	}
	if !strings.EqualFold(forwardURL.Scheme, "wss") {
		log.Fatalf("[客户端] 安全要求：仅支持 wss:// 协议 (当前: %s)", forwardURL.Scheme)
	}

	// 解析 IP 策略
	cfg.IPStrategy = parseIPStrategy(ips)
	if ips != "" {
		log.Printf("[客户端] IP 访问策略: %s (code: %d)", ips, cfg.IPStrategy)
	}

	// 解析目标 IP 列表
	var targetIPs []string
	if ipAddr != "" {
		for _, p := range strings.Split(ipAddr, ",") {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				targetIPs = append(targetIPs, trimmed)
			}
		}
	}

	// 解析 UDP 拦截端口列表
	if udpBlockPortsStr != "" {
		cfg.UDPBlockPorts = make(map[int]struct{})
		for _, p := range strings.Split(udpBlockPortsStr, ",") {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			var port int
			_, _ = fmt.Sscanf(pp, "%d", &port)
			if port > 0 && port < 65536 {
				cfg.UDPBlockPorts[port] = struct{}{}
			}
		}
		udpBlockPorts = cfg.UDPBlockPorts // 保持向后兼容
	}

	// 生成客户端 ID
	clientID = uuid.NewString()
	log.Printf("[客户端] 客户端ID: %s", clientID)

	// 设置运行时配置
	cfg.Insecure = insecure
	cfg.Token = token

	// 创建并启动连接池
	clientPool = NewClientPool(forwardAddr, connectionNum, targetIPs, clientID)
	clientPool.Start()

	// 监听退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动 SOCKS5 监听器
	var wg sync.WaitGroup
	for _, listenerRule := range listeners {
		rule := strings.TrimSpace(listenerRule)
		if rule == "" {
			continue
		}
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			runSOCKS5Listener(r)
		}(rule)
	}

	// 等待退出信号或监听器退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-sigChan:
		log.Printf("[客户端] 收到退出信号，正在优雅关闭...")
		clientPool.Shutdown()
		os.Exit(0)
	case <-done:
		// 所有监听器已退出
	}
}
