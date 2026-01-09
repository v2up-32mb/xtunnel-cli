//go:build client

package main

import (
	"errors"
	"io"
	"net"
	"strings"

	"github.com/gorilla/websocket"
)

// shortID 生成短 ID（用于日志输出）
//
// 参数:
//   - id: 原始连接 ID
//
// 返回前 8 个字符的短 ID
func shortID(id string) string {
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}

// isNormalCloseError 判断是否为正常的关闭错误
//
// 正常关闭的错误包括：
// - io.EOF、net.ErrClosed 等标准错误
// - WebSocket 正常关闭码（1000、1001、1005）
// - 网络超时错误
// - TLS 连接关闭相关的错误消息
//
// 参数:
//   - err: 待检查的错误
//
// 返回是否为正常的关闭错误
func isNormalCloseError(err error) bool {
	if err == nil {
		return false
	}
	// 标准关闭错误
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	// WebSocket 关闭错误
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived:
			return true
		}
	}
	// 网络超时
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// 检查错误消息（TLS 连接关闭等情况）
	errStr := err.Error()
	return strings.Contains(errStr, "tls: bad record MAC") ||
		strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused")
}
