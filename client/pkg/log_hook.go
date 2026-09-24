package client

// 客户端核心库约定：不直接向 stdout/stderr 输出日志。
// 所有需要展示的信息统一经 clientLogf 交给下游壳处理，由壳决定
// 输出目标、格式与级别；核心库默认静默。
// 壳在启动时调用 SetLogf 注入自己的日志函数即可接管全部客户端日志。

var clientLogf = func(string, ...any) {}

// SetLogf 由壳注入日志输出函数（如 log.Printf）。传入 nil 恢复静默。
func SetLogf(fn func(format string, args ...any)) {
	if fn == nil {
		clientLogf = func(string, ...any) {}
		return
	}
	clientLogf = fn
}
