//go:build client

package main

import (
	"sync"
	"time"
)

// ======================== 客户端配置 ========================
//
// GlobalConfig 包含客户端运行时的所有可配置参数

// GlobalConfig 客户端全局配置
type GlobalConfig struct {
	// DialTimeout TCP 连接超时
	DialTimeout time.Duration
	// WSHandshakeTimeout WebSocket 握手超时
	WSHandshakeTimeout time.Duration
	// WSWriteTimeout WebSocket 写入超时
	WSWriteTimeout time.Duration
	// WSReadTimeout WebSocket 读取超时
	WSReadTimeout time.Duration
	// PingInterval Ping 心跳间隔
	PingInterval time.Duration
	// ReconnectDelay 重连延迟
	ReconnectDelay time.Duration

	// ReadBuf32K 32KB 缓冲区大小
	ReadBuf32K int
	// ReadBuf64K 64KB 缓冲区大小
	ReadBuf64K int

	// Insecure 跳过证书验证
	Insecure bool
	// Token 身份验证令牌
	Token string
	// IPStrategy IP 策略
	IPStrategy byte
	// UDPBlockPorts UDP 拦截端口集合
	UDPBlockPorts map[int]struct{}
}

// cfg 全局配置实例（默认值）
var cfg = &GlobalConfig{
	DialTimeout:        3 * time.Second,
	WSHandshakeTimeout: 5 * time.Second,
	WSWriteTimeout:     5 * time.Second,
	WSReadTimeout:      10 * time.Second,
	PingInterval:       3 * time.Second,
	ReconnectDelay:     1 * time.Second,
	ReadBuf32K:         32 * 1024,
	ReadBuf64K:         64 * 1024,
	Insecure:           false,
	Token:              "",
	IPStrategy:         0,
	UDPBlockPorts:      nil,
}

// fallback 全局变量，用于指示是否使用Fallback模式 (Windows 7兼容性设置)
var fallback = true

// 缓冲区池（减少内存分配）
var buf32kPool = sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }}
var buf64kPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}
