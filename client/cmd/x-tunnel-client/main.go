package main

import (
	"flag"
	"fmt"
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

	// 启动本地代理监听器
	for _, addr := range listenAddrs {
		a := addr // 创建局部变量
		go func() {
			var err error
			switch {
			case strings.HasPrefix(a, "socks5://"):
				err = c.ListenSOCKS5(a)
			case strings.HasPrefix(a, "http://"):
				err = c.ListenHTTP(a)
			default:
				err = fmt.Errorf("不支持的监听协议")
			}
			if err != nil {
				log.Printf("[客户端] 监听器错误 (%s): %v", a, err)
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
	registerFlags(flag.CommandLine)
}

func registerFlags(fs *flag.FlagSet) {
	fs.StringVar(&configFile, "config", "", "JSON 配置文件路径（可选，CLI 参数优先级更高）")
	fs.StringVar(&listenAddr, "l", "", "监听地址 (支持 socks5:// 或 http://,支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080\n  http://[user:pass@]0.0.0.0:8080")
	fs.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式,必须是 wss://host:port/path)")
	fs.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（支持多种格式:IPv4, IPv4:PORT, IPv6, [IPv6]:PORT, 域名, 域名:PORT）,多个节点用逗号分隔")
	fs.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表,逗号分隔,如 443,8443")
	fs.BoolVar(&insecure, "insecure", false, "客户端 wss 模式忽略证书校验（启用后自动禁用 ECH）")
	fs.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	fs.StringVar(&dnsServer, "dns", "https://v.recipes/dns-query", "查询 ECH 公钥所用的 DNS 服务器 (支持 DoH 或 UDP)")
	fs.StringVar(&echDomain, "ech", "cloudflare-ech.com", "用于查询 ECH 公钥的域名")
	fs.BoolVar(&fallback, "fallback", false, "是否禁用 ECH 并回落到普通 TLS 1.3 (默认 false)")
	fs.IntVar(&connectionNum, "n", 3, "每个IP建立的WebSocket连接数量")
	fs.IntVar(&maxSOCKS5Connections, "max-socks5-conns", 1024, "SOCKS5 最大并发连接数，0 表示无限制")
	fs.DurationVar(&connectTimeout, "connect-timeout", 15*time.Second, "本地代理等待远端建链超时")
	fs.StringVar(&ips, "ips", "", "服务端解析目标地址的IP偏好\n 4: 仅IPv4\n 6: 仅IPv6\n 4,6: IPv4优先\n 6,4: IPv6优先")
	fs.BoolVar(&enableHotPair, "hotpair", false, "启用 Hot Channel Pair 降低首帧延迟")
	fs.IntVar(&hotPairCount, "hotpair-count", 1, "Hot Pair 数量")
	fs.DurationVar(&hotPairRefreshInterval, "hotpair-refresh", 30*time.Second, "Hot Pair 刷新间隔")
	fs.IntVar(&fastRetryAttempts, "fast-retry", 1, "快速重试次数")
	fs.DurationVar(&fastRetryWindow, "fast-retry-window", 1*time.Second, "快速重试窗口")
	fs.IntVar(&maxFastRetryConsecutive, "fast-retry-consecutive", 3, "连续进入快速重试的最大次数")
}

var (
	configFile              string
	listenAddr              string
	forwardAddr             string
	ipAddr                  string
	udpBlockPortsStr        string
	token                   string
	fallback                bool
	insecure                bool
	connectionNum           int
	maxSOCKS5Connections    int
	connectTimeout          time.Duration
	ips                     string
	dnsServer               string
	echDomain               string
	enableHotPair           bool
	hotPairCount            int
	hotPairRefreshInterval  time.Duration
	fastRetryAttempts       int
	fastRetryWindow         time.Duration
	maxFastRetryConsecutive int
)

func parseFlags() *client.Config {
	if err := applyClientFileConfig(configFile, visitedFlags()); err != nil {
		log.Fatalf("[客户端] 读取配置文件失败: %v", err)
	}
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

	cfg := client.DefaultConfig()
	cfg.ServerAddr = forwardAddr
	cfg.Token = token
	cfg.Connections = connectionNum
	cfg.RelayNodes = relayNodes
	cfg.EnableECH = enableECH
	cfg.ECHDomain = echDomain
	cfg.DNSServer = dnsServer
	cfg.InsecureSkipVerify = insecure
	cfg.IPStrategy = ipStrategy
	cfg.UDPBlockedPorts = udpBlockedPorts
	cfg.ConnectTimeout = connectTimeout
	cfg.MaxSOCKS5Connections = maxSOCKS5Connections
	cfg.EnableHotPair = enableHotPair
	cfg.HotPairCount = hotPairCount
	cfg.HotPairRefreshInterval = hotPairRefreshInterval
	cfg.FastRetryAttempts = fastRetryAttempts
	cfg.FastRetryWindow = fastRetryWindow
	cfg.MaxFastRetryConsecutive = maxFastRetryConsecutive

	// 生成并复用客户端 ID
	cfg.ClientID = uuid.NewString()
	log.Printf("[客户端] 客户端ID: %s", cfg.ClientID)

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
		if !strings.HasPrefix(l, "socks5://") && !strings.HasPrefix(l, "http://") {
			log.Fatalf("[客户端] 仅支持 SOCKS5/HTTP 监听:非法监听地址 %q", l)
		}
		listeners = append(listeners, l)
	}

	return listeners
}
