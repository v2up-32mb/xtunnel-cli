//go:build client
// +build client

package main

import (
	"flag"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// GlobalConfig 客户端全局配置
type GlobalConfig struct {
	DialTimeout        time.Duration
	WSHandshakeTimeout time.Duration
	WSWriteTimeout     time.Duration
	WSReadTimeout      time.Duration
	PingInterval       time.Duration
	ReconnectDelay     time.Duration

	ReadBuf32K int
	ReadBuf64K int
}

var cfg = GlobalConfig{
	DialTimeout:        3 * time.Second,
	WSHandshakeTimeout: 5 * time.Second,
	WSWriteTimeout:     5 * time.Second,
	WSReadTimeout:      15 * time.Second, // 15秒超时
	PingInterval:       5 * time.Second,  // 5秒ping一次
	ReconnectDelay:     1 * time.Second,
	ReadBuf32K:         32 * 1024,
	ReadBuf64K:         64 * 1024,
}

var buf32kPool = sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }}
var buf64kPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// ======================== 客户端参数 ========================

var (
	listenAddr       string
	forwardAddr      string
	ipAddr           string
	udpBlockPortsStr string
	token            string
	fallback         bool
	insecure         bool
	connectionNum    int
	ips              string

	dnsServer string
	echDomain string

	echListMu sync.RWMutex
	echList   []byte
	refreshMu sync.Mutex

	echPool *ECHPool

	clientID      string
	udpBlockPorts map[int]struct{}
	ipStrategy    byte
)

func init() {
	flag.StringVar(&listenAddr, "l", "", "监听地址 (仅支持 socks5://，支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080")
	flag.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式，必须是 wss://host:port/path)")
	flag.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（支持多种格式：IPv4, IPv4:PORT, IPv6, [IPv6]:PORT, 域名, 域名:PORT），多个节点用逗号分隔")
	flag.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表，逗号分隔，如 443,8443")
	flag.BoolVar(&insecure, "insecure", false, "客户端 wss 模式忽略证书校验（启用后自动禁用 ECH）")
	flag.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	flag.StringVar(&dnsServer, "dns", "https://doh.pub/dns-query", "查询 ECH 公钥所用的 DNS 服务器 (支持 DoH 或 UDP)")
	flag.StringVar(&echDomain, "ech", "cloudflare-ech.com", "用于查询 ECH 公钥的域名")
	flag.BoolVar(&fallback, "fallback", false, "是否禁用 ECH 并回落到普通 TLS 1.3 (默认 false)")
	flag.IntVar(&connectionNum, "n", 3, "每个IP建立的WebSocket连接数量")
	flag.StringVar(&ips, "ips", "", "服务端解析目标地址的IP偏好\n 4: 仅IPv4\n 6: 仅IPv6\n 4,6: IPv4优先\n 6,4: IPv6优先")
}

func main() {
	log.Printf("[客户端] 程序启动")
	flag.Parse()
	log.Printf("[客户端] 参数解析完成")

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

	ipStrategy = parseIPStrategy(ips)
	if ips != "" {
		log.Printf("[客户端] IP 访问策略: %s (code: %d)", ips, ipStrategy)
	}

	defaultPort := "443"
	if u, err := url.Parse(forwardAddr); err == nil {
		if _, port, err := net.SplitHostPort(u.Host); err == nil {
			defaultPort = port
		}
	}

	// wss 模式：如果开启不校验证书，则自动禁用 ECH
	if insecure {
		if !fallback {
			fallback = true
			log.Printf("[客户端] 启用 -insecure：已自动禁用 ECH（fallback）")
		} else {
			log.Printf("[客户端] 启用 -insecure")
		}
	}

	if !fallback {
		if err := prepareECH(); err != nil {
			log.Fatalf("[客户端] 获取 ECH 公钥失败: %v", err)
		}
	} else {
		log.Printf("[客户端] fallback 模式已启用：禁用 ECH，使用标准 TLS 1.3")
	}

	if udpBlockPortsStr != "" {
		udpBlockPorts = make(map[int]struct{})
		for _, p := range strings.Split(udpBlockPortsStr, ",") {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			port, err := strconv.Atoi(pp)
			if err == nil && port > 0 && port < 65536 {
				udpBlockPorts[port] = struct{}{}
			}
		}
	}

	clientID = uuid.NewString()
	log.Printf("[客户端] 客户端ID: %s", clientID)

	echPool = NewECHPool(forwardAddr, connectionNum, nil, clientID)

	if ipAddr != "" {
		for _, addr := range strings.Split(ipAddr, ",") {
			trimmed := strings.TrimSpace(addr)
			if trimmed == "" {
				continue
			}
			addedIPs, err := echPool.relayManager.AddNodeAndTest(trimmed, defaultPort)
			if err != nil {
				log.Printf("[客户端] 添加中转节点 %s 失败: %v", trimmed, err)
			} else {
				for _, ip := range addedIPs {
					node := echPool.relayManager.GetNodeByIP(ip)
					if node != nil {
						latency := node.Latency.Milliseconds()
						log.Printf("[客户端] 中转节点: %s 已添加, 连接延迟: %dms", ip, latency)
					} else {
						log.Printf("[客户端] 中转节点: %s 已添加", ip)
					}
				}
			}
		}
	}

	echPool.Start()

	// 监听退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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
		echPool.Shutdown()
		os.Exit(0)
	case <-done:
		// 所有监听器已退出
	}
}
