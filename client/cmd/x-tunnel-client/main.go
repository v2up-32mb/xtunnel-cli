package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	xsharedrouting "github.com/v2up-32mb/xshared/routing"
	"github.com/v2up-32mb/xtunnel"
	"github.com/v2up-32mb/xtunnel/protocol"
)

func main() {
	log.Printf("[客户端] 程序启动")
	flag.Parse()

	cfg := parseFlags()

	// 构建路由绕过 matcher
	if bypassPrivate || bypassGeoIPCN || bypassGeoSiteCN || strings.TrimSpace(bypassRules) != "" {
		geoIPPathResolved := resolveGeoPath(geoIPPath, "geoip.dat")
		geoSitePathResolved := resolveGeoPath(geoSitePath, "geosite.dat")
		m, err := xsharedrouting.NewMatcherWithGeoFile(bypassPrivate, bypassGeoIPCN, bypassGeoSiteCN, bypassRules, geoIPPathResolved, geoSitePathResolved)
		if err != nil {
			log.Fatalf("[客户端] 构建路由绕过 matcher 失败: %v", err)
		}
		bypassMatcher = m
		geoLoaded := ""
		if geoIPPathResolved != "" {
			geoLoaded += "geoip.dat=已加载 "
		}
		if geoSitePathResolved != "" {
			geoLoaded += "geosite.dat=已加载 "
		}
		if geoLoaded == "" {
			geoLoaded = "使用内置数据"
		}
		log.Printf("[客户端] 路由绕过已启用: private=%v geoip-cn=%v geosite-cn=%v rules=%q | %s", bypassPrivate, bypassGeoIPCN, bypassGeoSiteCN, bypassRules, strings.TrimSpace(geoLoaded))
	}

	c, err := xtunnel.NewClient(cfg)
	if err != nil {
		log.Fatalf("[客户端] 创建客户端失败: %v", err)
	}

	// 先解析并校验本地监听地址，避免连接池已启动后才发现地址非法
	listenAddrs := parseListenAddrs()

	if err := c.Start(); err != nil {
		log.Fatalf("[客户端] 启动客户端失败: %v", err)
	}
	defer c.Shutdown()

	if reverseMode {
		log.Printf("[客户端] 反向模式：监听将由服务端按 -l 参数开启")
	} else {
		// 启动本地代理监听器
		for _, addr := range listenAddrs {
			a := addr // 创建局部变量
			go func() {
				var err error
				switch {
				case strings.HasPrefix(a, "socks5://"):
					err = startSocks5Listener(a, c)
				case strings.HasPrefix(a, "http://"):
					err = startHTTPListener(a, c)
				default:
					err = fmt.Errorf("不支持的监听协议")
				}
				if err != nil {
					// 监听失败（端口占用/协议错误）为致命错误，直接退出
					log.Fatalf("[客户端] 监听器启动失败 (%s): %v", a, err)
				}
			}()
		}
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
	fs.BoolVar(&enableHotPair, "hotpair", false, "启用 Hot Channel Pair 降低首帧延迟（正向=本地预热；反向=通知服务端预热）")
	fs.IntVar(&hotPairCount, "hotpair-count", 1, "Hot Pair 数量")
	fs.DurationVar(&hotPairRefreshInterval, "hotpair-refresh", 30*time.Second, "Hot Pair 刷新间隔")
	fs.IntVar(&fastRetryAttempts, "fast-retry", 1, "快速重试次数")
	fs.DurationVar(&fastRetryWindow, "fast-retry-window", 1*time.Second, "快速重试窗口")
	fs.IntVar(&maxFastRetryConsecutive, "fast-retry-consecutive", 3, "连续进入快速重试的最大次数")
	fs.IntVar(&backpressureLimitBytes, "backpressure-limit", 1024*1024, "全局队列背压阈值（字节），默认 1MB")
	fs.BoolVar(&bypassPrivate, "bypass-private", false, "绕过私有/局域网地址（直连不走隧道）")
	fs.BoolVar(&bypassGeoIPCN, "bypass-geoip-cn", false, "绕过中国大陆 IP（内置 GeoIP 规则，直连）")
	fs.BoolVar(&bypassGeoSiteCN, "bypass-geosite-cn", false, "绕过中国大陆域名（内置 GeoSite 规则，直连）")
	fs.StringVar(&bypassRules, "bypass-rules", "", "自定义绕过规则（多行，支持 domain:/full:/IP/CIDR）")
	fs.StringVar(&geoIPPath, "geo-ip", "", "geoip.dat 路径（v2ray 格式，覆盖内置 CN 段；留空探测程序同目录）")
	fs.StringVar(&geoSitePath, "geo-site", "", "geosite.dat 路径（v2ray 格式，覆盖内置 CN 域名；留空探测程序同目录）")
	fs.BoolVar(&reverseMode, "reverse", false, "启用反向模式：-l 参数不再开启本地监听，而是由服务端监听")
	fs.BoolVar(&reverseMode, "r", false, "启用反向模式：-l 参数不再开启本地监听，而是由服务端监听")
}

var (
	udpBlockedPorts         []int
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
	backpressureLimitBytes  int
	bypassPrivate           bool
	bypassGeoIPCN           bool
	bypassGeoSiteCN         bool
	bypassRules             string
	geoIPPath               string
	geoSitePath             string
	reverseMode             bool
)

var bypassMatcher *xsharedrouting.Matcher

func parseFlags() *xtunnel.Config {
	if err := applyClientFileConfig(configFile, visitedFlags()); err != nil {
		log.Fatalf("[客户端] 读取配置文件失败: %v", err)
	}
	if listenAddr == "" || forwardAddr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// 解析 UDP 拦截端口
	udpBlockedPorts = nil
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
	ipStrategy := protocol.IPStrategyDefault
	if ips != "" {
		var err error
		ipStrategy, err = protocol.ParseIPStrategy(ips)
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

	cfg := xtunnel.DefaultConfig()
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
	cfg.BackpressureLimitBytes = backpressureLimitBytes

	cfg.EnableReverse = reverseMode
	if reverseMode {
		cfg.ReverseListeners = parseListenAddrs()
		cfg.OnReverseError = func(err error) {
			log.Fatalf("[客户端] %v", err)
		}
	}

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

// resolveGeoPath 解析 geoip/geosite dat 路径：显式 flag 优先；留空时探测可执行文件同目录；文件不存在返回空（库内静默回退内置）
func resolveGeoPath(flagVal, defaultName string) string {
	if p := strings.TrimSpace(flagVal); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		// flag 指定但文件不存在，回退为空
		return ""
	}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		p := filepath.Join(dir, defaultName)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}
