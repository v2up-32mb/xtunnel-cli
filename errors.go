//go:build client

package main

import "fmt"

// ======================== 错误定义 ========================
//
// 本包中使用的自定义错误类型

var (
	// ErrOnlyWSS 仅支持 WSS 协议错误
	//
	// 客户端和服务端都只支持 wss:// 协议，不支持 ws://
	ErrOnlyWSS = fmt.Errorf("仅支持 wss:// 协议")

	// ErrAuthFailed 认证失败错误
	//
	// Token 不匹配或未提供时返回此错误
	ErrAuthFailed = fmt.Errorf("认证失败：Token 不匹配或未提供")
)
