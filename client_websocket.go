//go:build client

package main

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
)

// ======================== WebSocket 连接 ========================

// buildTLSConfig 构建 TLS 配置
//
// 创建一个 TLS 1.3 配置，使用系统根证书池。
// 可以通过全局变量 insecure 跳过证书验证（仅用于测试）。
//
// 参数:
//   - serverName: TLS 服务器名称（用于 SNI）
//
// 返回 TLS 配置和可能的错误
func buildTLSConfig(serverName string) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: cfg.Insecure,
	}, nil
}

// dialWebSocket 建立 WebSocket 连接（仅支持 wss://）
//
// 连接参数通过 URL 查询字符串传递：
//   - client_id: 客户端唯一标识
//   - ch_id: 通道 ID
//
// Token 通过 WebSocket Subprotocol 头传递。
//
// 参数:
//   - addr: WebSocket 服务器地址（wss://host:port/path）
//   - ip: 指定连接的目标 IP（可选，用于 SNI 和实际连接分离）
//   - clientID: 客户端唯一标识
//   - chID: 通道 ID
//
// 返回 WebSocket 连接和可能的错误
func dialWebSocket(addr string, ip string, clientID string, chID int) (*websocket.Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	// 仅支持 wss:// 协议
	if !strings.EqualFold(u.Scheme, "wss") {
		return nil, ErrOnlyWSS
	}

	// 构建 WebSocket 连接 URL
	dialURL := *u
	q := dialURL.Query()
	if clientID != "" {
		q.Set("client_id", clientID)
	}
	q.Set("ch_id", strconv.Itoa(chID))
	dialURL.RawQuery = q.Encode()
	dialAddr := dialURL.String()

	// 构建 TLS 配置（使用 URL 中的 hostname 作为 SNI）
	serverName := u.Hostname()
	tlsCfg, err := buildTLSConfig(serverName)
	if err != nil {
		return nil, err
	}

	// 配置 WebSocket 拨号器
	dialer := websocket.Dialer{
		TLSClientConfig:  tlsCfg,
		HandshakeTimeout: cfg.WSHandshakeTimeout,
		ReadBufferSize:   cfg.ReadBuf64K,
		WriteBufferSize:  cfg.ReadBuf64K,
	}
	if cfg.Token != "" {
		dialer.Subprotocols = []string{cfg.Token}
	}

	// 如果指定了 IP，使用自定义拨号函数
	// 这允许连接到特定 IP，但使用原域名进行 TLS SNI
	if ip != "" {
		dialer.NetDial = func(network, address string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(address)
			return net.DialTimeout(network, net.JoinHostPort(ip, port), cfg.DialTimeout)
		}
	}

	conn, resp, err := dialer.Dial(dialAddr, nil)
	if err != nil {
		// 检查是否为认证失败
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return nil, ErrAuthFailed
		}
		return nil, err
	}
	return conn, nil
}
