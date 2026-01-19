package client

import (
	"errors"
	"time"

	"x-tunnel/common"
)

// 各种错误定义
var (
	ErrInvalidServerAddr   = errors.New("invalid server address")
	ErrInvalidConnections  = errors.New("connections must be positive")
	ErrInvalidTimeout      = errors.New("timeout must be positive")
	ErrClientNotStarted    = errors.New("client not started")
	ErrClientAlreadyClosed = errors.New("client already closed")
)

// Config 客户端配置
type Config struct {
	// 服务端配置
	ServerAddr  string // WebSocket 服务器地址 (wss://...)
	Token       string // 认证令牌
	Connections int    // 每个 WebSocket 连接数量

	// 网络配置
	DialTimeout      time.Duration // 拨号超时
	HandshakeTimeout time.Duration // 握手超时
	ReadTimeout      time.Duration // 读超时
	WriteTimeout     time.Duration // 写超时
	PingInterval     time.Duration // Ping 间隔
	ReconnectDelay   time.Duration // 重连延迟

	// ECH 配置
	EnableECH          bool          // 是否启用 ECH
	ECHDomain          string        // ECH 查询域名
	DNSServer          string        // DNS 服务器
	InsecureSkipVerify bool          // 是否跳过证书验证

	// IP 策略
	IPStrategy common.IPStrategy // IP 地址解析策略

	// 中转节点
	RelayNodes []string // 中转节点列表

	// UDP 拦截端口
	UDPBlockedPorts []int // UDP 拦截端口列表

	// 缓冲区大小
	ReadBufferSize  int // 读缓冲区大小
	WriteBufferSize int // 写缓冲区大小
}

// DefaultConfig 返回带有合理默认值的配置
func DefaultConfig() *Config {
	return &Config{
		Connections:       3,
		DialTimeout:       3 * time.Second,
		HandshakeTimeout:  5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      5 * time.Second,
		PingInterval:      5 * time.Second,
		ReconnectDelay:    1 * time.Second,
		EnableECH:         true,
		ECHDomain:         "cloudflare-ech.com",
		DNSServer:         "https://doh.pub/dns-query",
		IPStrategy:        common.IPStrategyDefault,
		ReadBufferSize:    64 * 1024,
		WriteBufferSize:   64 * 1024,
		UDPBlockedPorts:   []int{443},
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.ServerAddr == "" {
		return ErrInvalidServerAddr
	}

	if c.Connections <= 0 {
		return ErrInvalidConnections
	}

	if c.DialTimeout <= 0 {
		return ErrInvalidTimeout
	}

	return nil
}
