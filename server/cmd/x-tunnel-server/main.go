package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/v2up-32mb/xtunnel"
)

// renderServerLog 壳接管服务端核心日志：按 -log-level 过滤后按等级/模块渲染
func renderServerLog(ev xtunnel.LogEvent) {
	if ev.Level < xtunnel.LogLevel(minLogLevel.Load()) {
		return
	}
	level := "DEBUG"
	switch ev.Level {
	case xtunnel.LevelInfo:
		level = "INFO"
	case xtunnel.LevelWarn:
		level = "WARN"
	case xtunnel.LevelError:
		level = "ERROR"
	}
	log.Printf("[%s][%s] %s", level, ev.Module, fmt.Sprintf(ev.Format, ev.Args...))
}

// minLogLevel 壳最低输出等级（xtunnel.LogLevel 值），由 -log-level 设置。
var minLogLevel atomic.Int32

// setMinLogLevel 解析 -log-level 并设置最低输出等级；非法值直接 Fatal。
func setMinLogLevel() {
	switch strings.ToLower(strings.TrimSpace(logLevel)) {
	case "", "debug":
		minLogLevel.Store(int32(xtunnel.LevelDebug))
	case "info":
		minLogLevel.Store(int32(xtunnel.LevelInfo))
	case "warn":
		minLogLevel.Store(int32(xtunnel.LevelWarn))
	case "error":
		minLogLevel.Store(int32(xtunnel.LevelError))
	default:
		shellFatalf("[服务端] 非法日志等级 %q (可选: debug/info/warn/error)", logLevel)
	}
}

// shellLog 壳自身日志：统一显示风格 [等级][shell]，受 -log-level 过滤
func shellLog(level string, format string, args ...any) {
	var lvl xtunnel.LogLevel
	switch level {
	case "DEBUG":
		lvl = xtunnel.LevelDebug
	case "WARN":
		lvl = xtunnel.LevelWarn
	case "ERROR":
		lvl = xtunnel.LevelError
	default:
		lvl = xtunnel.LevelInfo
	}
	if lvl < xtunnel.LogLevel(minLogLevel.Load()) {
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
	// 服务端核心默认静默，由壳接管全部日志输出
	xtunnel.SetLogf(renderServerLog)
	flag.Parse()
	setMinLogLevel() // -log-level 解析（非法则退出）

	shellLog("INFO", "[服务端] 程序启动")
	cfg := parseFlags()

	s, err := xtunnel.NewServer(cfg)
	if err != nil {
		shellFatalf("[服务端] 创建服务端失败: %v", err)
	}

	if err := s.Start(); err != nil {
		shellFatalf("[服务端] 启动服务端失败: %v", err)
	}
	defer s.Shutdown()

	shellLog("INFO", "[服务端] 已启动,等待连接...")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	shellLog("INFO", "[服务端] 收到退出信号,正在关闭...")
}

func init() {
	registerFlags(flag.CommandLine)
}

func registerFlags(fs *flag.FlagSet) {
	fs.StringVar(&logLevel, "log-level", "info", "日志等级: debug/info/warn/error")
	fs.StringVar(&configFile, "config", "", "JSON 配置文件路径（可选，CLI 参数优先级更高）")
	fs.StringVar(&listenAddr, "l", ":8443", "监听地址")
	fs.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	fs.StringVar(&certFile, "cert", "", "TLS 证书文件 (不指定则自动生成自签证书)")
	fs.StringVar(&keyFile, "key", "", "TLS 私钥文件 (不指定则自动生成自签证书)")
	fs.IntVar(&maxTotalChannels, "max-total-channels", 0, "服务端最大总通道数，0 表示无限制")
	fs.IntVar(&maxChannelsPerClient, "max-client-channels", 0, "每个客户端最大通道数，0 表示无限制")
	fs.IntVar(&backpressureLimitBytes, "backpressure-limit", 32<<20, "全局队列背压阈值（字节），默认 32MB")
	fs.IntVar(&maxReverseListeners, "max-reverse-listeners", 3, "每个客户端最大反向监听器数，默认 3")
}

var (
	logLevel               string
	configFile             string
	listenAddr             string
	token                  string
	certFile               string
	keyFile                string
	maxTotalChannels       int
	maxChannelsPerClient   int
	backpressureLimitBytes int
	maxReverseListeners    int
)

func parseFlags() *xtunnel.ServerConfig {
	if err := applyServerFileConfig(configFile, visitedFlags()); err != nil {
		shellFatalf("[服务端] 读取配置文件失败: %v", err)
	}
	if token == "" {
		shellFatalf("[服务端] 错误: 必须指定 -token 参数")
	}

	// 确定是否使用自签证书；cert 与 key 必须成对提供，否则报错
	autoCert := true
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			shellFatalf("[服务端] 错误: -cert 与 -key 必须同时提供")
		}
		autoCert = false
	}

	cfg := xtunnel.DefaultServerConfig()
	cfg.ListenAddr = listenAddr
	cfg.Token = token
	cfg.CertFile = certFile
	cfg.KeyFile = keyFile
	cfg.AutoCert = autoCert
	cfg.MaxTotalChannels = maxTotalChannels
	cfg.MaxChannelsPerClient = maxChannelsPerClient
	cfg.BackpressureLimitBytes = backpressureLimitBytes
	cfg.MaxReverseListeners = maxReverseListeners

	return cfg
}
