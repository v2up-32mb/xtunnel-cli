package client

import (
	"sync/atomic"
	"time"
)

// 客户端核心库约定：不直接向 stdout/stderr 输出日志。
// 所有需要展示的信息统一经 clientLogHook 以 LogEvent 形式交给壳处理，
// 壳负责按等级过滤、按模块路由并决定输出目标与格式；核心库默认静默。

// LogLevel 日志等级
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// LogEvent 一条库内日志事件
type LogEvent struct {
	Level  LogLevel
	Module string // 来源模块：pool / pair_warmer / relay / reverse / client ...
	Time   time.Time
	Format string
	Args   []any
}

type clientLogHookFunc func(LogEvent)

// clientLogHook 用 atomic.Value 存储，允许运行时安全切换（不影响并发日志调用）。
var clientLogHook atomic.Value // stores clientLogHookFunc

func init() {
	clientLogHook.Store(clientLogHookFunc(func(LogEvent) {}))
}

// SetLogf 由壳注入日志事件处理函数；传入 nil 恢复静默。
// 可随时调用（原子切换）；注入的 hook 必须并发安全。
func SetLogf(fn func(ev LogEvent)) {
	if fn == nil {
		clientLogHook.Store(clientLogHookFunc(func(LogEvent) {}))
		return
	}
	clientLogHook.Store(clientLogHookFunc(fn))
}

// clientLog 库内统一日志入口（带等级与模块元数据）
func clientLog(level LogLevel, module, format string, args ...any) {
	clientLogHook.Load().(clientLogHookFunc)(LogEvent{Level: level, Module: module, Time: time.Now(), Format: format, Args: args})
}
