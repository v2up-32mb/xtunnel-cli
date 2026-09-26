package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"x-tunnel/client/pkg"
	"x-tunnel/common"
)

// renderClientLog 壳接管核心库日志：按 -log-level 过滤后按等级/模块渲染
func renderClientLog(ev client.LogEvent) {
	if ev.Level < client.LogLevel(minLogLevel.Load()) {
		return
	}
	level := "DEBUG"
	switch ev.Level {
	case client.LevelInfo:
		level = "INFO"
	case client.LevelWarn:
		level = "WARN"
	case client.LevelError:
		level = "ERROR"
	}
	log.Printf("[%s][%s] %s", level, ev.Module, fmt.Sprintf(ev.Format, ev.Args...))
}

// minLogLevel 壳最低输出等级（client.LogLevel 值），由 -log-level 设置。
var minLogLevel atomic.Int32

// setMinLogLevel 解析 -log-level 并设置最低输出等级；非法值直接 Fatal。
func setMinLogLevel() {
	switch strings.ToLower(strings.TrimSpace(logLevel)) {
	case "", "debug":
		minLogLevel.Store(int32(client.LevelDebug))
	case "info":
		minLogLevel.Store(int32(client.LevelInfo))
	case "warn":
		minLogLevel.Store(int32(client.LevelWarn))
	case "error":
		minLogLevel.Store(int32(client.LevelError))
	default:
		shellFatalf("[客户端] 非法日志等级 %q (可选: debug/info/warn/error)", logLevel)
	}
}

// shellLog 壳自身日志：与核心事件统一显示风格 [等级][shell] 消息，受 -log-level 过滤
func shellLog(level string, format string, args ...any) {
	var lvl client.LogLevel
	switch level {
	case "DEBUG":
		lvl = client.LevelDebug
	case "WARN":
		lvl = client.LevelWarn
	case "ERROR":
		lvl = client.LevelError
	default:
		lvl = client.LevelInfo
	}
	if lvl < client.LogLevel(minLogLevel.Load()) {
		return
	}
	log.Printf("[%s][shell] %s", level, fmt.Sprintf(format, args...))
}

// shellFatalf 壳自身致命错误：ERROR 级日志后退出
func shellFatalf(format string, args ...any) {
	shellLog("ERROR", format, args...)
	os.Exit(1)
}

func main() {
	// 核心库静默，由壳接管全部日志输出（等级/模块/格式由壳决定）
	client.SetLogf(renderClientLog)
	flag.Parse()
	setMinLogLevel()
	shellLog("INFO", "[客户端] 程序启动")

	cfg := parseFlags()

	c, err := client.NewClient(cfg)
	if err != nil {
		shellFatalf("[客户端] 创建客户端失败: %v", err)
	}

	// 先解析并校验本地监听地址，避免连接池已启动后才发现地址非法
	listenAddrs := parseListenAddrs()

	if err := c.Start(); err != nil {
		shellFatalf("[客户端] 启动客户端失败: %v", err)
	}
	defer c.Shutdown()

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
				// 监听失败（端口占用/协议错误）为致命错误，直接退出
				shellFatalf("[客户端] 监听器启动失败 (%s): %v", a, err)
			}
		}()
	}

	shellLog("INFO", "[客户端] 已启动,等待连接...")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	shellLog("INFO", "[客户端] 收到退出信号,正在关闭...")
}

func init() {
	registerFlags(flag.CommandLine)
}

func registerFlags(fs *flag.FlagSet) {
	fs.StringVar(&logLevel, "log-level", "info", "日志等级: debug/info/warn/error")
	fs.StringVar(&configFile, "config", "", "JSON 配置文件路径（可选，CLI 参数优先级更高）")
	fs.StringVar(&listenAddr, "l", "", "监听地址 (支持 socks5:// 或 http://,支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080\n  http://[user:pass@]0.0.0.0:8080")
	fs.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式,必须是 wss://host:port/path)")
	fs.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（支持多种格式:IPv4, IPv4:PORT, IPv6, [IPv6]:PORT, 域名, 域名:PORT）,多个节点用逗号分隔")
	fs.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表,逗号分隔,如 443,8443")
	fs.BoolVar(&insecure, "insecure", false, "客户端 wss 模式忽略证书校验")
	fs.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
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
	fs.IntVar(&backpressureLimitBytes, "backpressure-limit", 1024*1024, "全局队列背压阈值（字节），默认 1MB")
}

var (
	logLevel                string
	configFile              string
	listenAddr              string
	forwardAddr             string
	ipAddr                  string
	udpBlockPortsStr        string
	token                   string
	insecure                bool
	connectionNum           int
	maxSOCKS5Connections    int
	connectTimeout          time.Duration
	ips                     string
	enableHotPair           bool
	hotPairCount            int
	hotPairRefreshInterval  time.Duration
	fastRetryAttempts       int
	fastRetryWindow         time.Duration
	maxFastRetryConsecutive int
	backpressureLimitBytes  int
)

func parseFlags() *client.Config {
	if err := applyClientFileConfig(configFile, visitedFlags()); err != nil {
		shellFatalf("[客户端] 读取配置文件失败: %v", err)
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
			shellLog("WARN", "[客户端] IP 策略解析失败: %v,使用默认策略", err)
		} else {
			shellLog("INFO", "[客户端] IP 访问策略: %s (code: %d)", ips, ipStrategy)
		}
	}

	cfg := client.DefaultConfig()
	cfg.ServerAddr = forwardAddr
	cfg.Token = token
	cfg.Connections = connectionNum
	cfg.RelayNodes = relayNodes
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

	// 生成并复用客户端 ID
	cfg.ClientID = uuid.NewString()
	shellLog("INFO", "[客户端] 客户端ID: %s", cfg.ClientID)

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
			shellFatalf("[客户端] 仅支持 SOCKS5/HTTP 监听:非法监听地址 %q", l)
		}
		listeners = append(listeners, l)
	}

	return listeners
}
