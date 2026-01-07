//go:build client || server
// +build client || server

package main

import (
	"errors"
	"io"
	"net"

	"github.com/gorilla/websocket"
)

// shortID 返回短格式的连接 ID（用于日志）
func shortID(id string) string {
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}

// isNormalCloseError 判断是否为正常的关闭错误
func isNormalCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived:
			return true
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}
