package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"x-tunnel/client/pkg"
	"x-tunnel/common"
)

func main() {
	log.Printf("[客户端] 程序启动")
	flag.Parse()

	cfg := parseFlags()

	c, err := client.NewClient(cfg)
	if err != nil {
		log.Fatalf("[客户端] 创建客户端失败: %v", err)
	}

	if err := c.Start(); err != nil {
		log.Fatalf("[客户端] 启动客户端失败: %v", err)
	}
	defer c.Shutdown()

	// 解析监听地址
	listenAddrs := parseListenAddrs()

	// 启动 SOCKS5 监听器
	for _, addr := range listenAddrs {
		a := addr // 创建局部变量
		go func() {
			if err := c.ListenSOCKS5(a); err != nil {
				log.Printf("[客户端] SOCKS5 监听器错误 (%s): %v", a, err)
			}
		}()
	}

	log.Printf("[客户端] 已启动,等待连接...")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("[客户端] 收到退出信号,正在关闭...")
}

func init() {
	flag.StringVar(&listenAddr, "l", "", "监听地址 (仅支持 socks5://,支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080")
	flag.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式,必须是 wss://host:port/path)")
	flag.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（支持多种格式:IPv4, IPv4:PORT, IPv6, [IPv6]:PORT, 域名, 域名:PORT）,多个节点用逗号分隔")
	flag.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表,逗号分隔,如 443,8443")
	flag.BoolVar(&insecure, "insecure", false, "客户端 wss 模式忽略证书校验（启用后自动禁用 ECH）")
	flag.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	flag.StringVar(&dnsServer, "dns", "https://doh.pub/dns-query", "查询 ECH 公钥所用的 DNS 服务器 (支持 DoH 或 UDP)")
	flag.StringVar(&echDomain, "ech", "cloudflare-ech.com", "用于查询 ECH 公钥的域名")
	flag.BoolVar(&fallback, "fallback", false, "是否禁用 ECH 并回落到普通 TLS 1.3 (默认 false)")
	flag.IntVar(&connectionNum, "n", 3, "每个IP建立的WebSocket连接数量")
	flag.StringVar(&ips, "ips", "", "服务端解析目标地址的IP偏好\n 4: 仅IPv4\n 6: 仅IPv6\n 4,6: IPv4优先\n 6,4: IPv6优先")
}

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
	dnsServer        string
	echDomain        string
)

func parseFlags() *client.Config {
	if listenAddr == "" || forwardAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// 解析 UDP 拦截端口
	var udpBlockedPorts []int
	if udpBlockPortsStr != "" {
		for _, p := range strings.Split(udpBlockPortsStr, ",") {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			port, err := strconv.Atoi(pp)
			if err == nil && port > 0 && port < 65536 {
				udpBlockedPorts = append(udpBlockedPorts, port)
			}
		}
	}

	// 解析中转节点
	var relayNodes []string
	if ipAddr != "" {
		for _, addr := range strings.Split(ipAddr, ",") {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				relayNodes = append(relayNodes, trimmed)
			}
		}
	}

	// 解析 IP 策略
	ipStrategy := common.IPStrategyDefault
	if ips != "" {
		var err error
		ipStrategy, err = common.ParseIPStrategy(ips)
		if err != nil {
			log.Printf("[客户端] IP 策略解析失败: %v,使用默认策略", err)
		} else {
			log.Printf("[客户端] IP 访问策略: %s (code: %d)", ips, ipStrategy)
		}
	}

	// 处理 insecure 和 fallback
	enableECH := !fallback
	if insecure {
		if !fallback {
			fallback = true
			log.Printf("[客户端] 启用 -insecure:已自动禁用 ECH（fallback）")
		}
		enableECH = false
	}

	cfg := &client.Config{
		ServerAddr:        forwardAddr,
		Token:             token,
		Connections:       connectionNum,
		RelayNodes:        relayNodes,
		EnableECH:         enableECH,
		ECHDomain:         echDomain,
		DNSServer:         dnsServer,
		InsecureSkipVerify: insecure,
		IPStrategy:        ipStrategy,
		UDPBlockedPorts:   udpBlockedPorts,
		DialTimeout:       3 * time.Second,
		HandshakeTimeout:  5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      5 * time.Second,
		PingInterval:      5 * time.Second,
		ReconnectDelay:    1 * time.Second,
		ReadBufferSize:    64 * 1024,
		WriteBufferSize:   64 * 1024,
	}

	// 生成客户端 ID
	clientID := uuid.NewString()
	log.Printf("[客户端] 客户端ID: %s", clientID)

	return cfg
}

func parseListenAddrs() []string {
	var listeners []string

	if listenAddr == "" {
		return listeners
	}

	for _, l := range strings.Split(listenAddr, ",") {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if !strings.HasPrefix(l, "socks5://") {
			log.Fatalf("[客户端] 仅支持 SOCKS5 监听:非法监听地址 %q", l)
		}
		listeners = append(listeners, l)
	}

	return listeners
}
