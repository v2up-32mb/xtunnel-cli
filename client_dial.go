//go:build client
// +build client

package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// dialWebSocketWithECH 建立 WebSocket 连接（支持 ECH）
func dialWebSocketWithECH(addr string, retries int, ip string, clientID string, chID int) (*websocket.Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(u.Scheme, "wss") {
		return nil, fmt.Errorf("仅支持 wss:// (当前: %s)", u.Scheme)
	}

	dialURL := *u
	q := dialURL.Query()
	if clientID != "" {
		q.Set("client_id", clientID)
	}
	// 告诉服务端期望的通道 ID
	q.Set("ch_id", fmt.Sprintf("%d", chID))
	dialURL.RawQuery = q.Encode()
	dialAddr := dialURL.String()

	serverName := u.Hostname()
	for i := 1; i <= retries; i++ {
		tlsCfg, e := buildUnifiedTLSConfig(serverName)
		if e != nil {
			if i < retries {
				_ = refreshECH()
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, e
		}

		dialer := websocket.Dialer{
			TLSClientConfig:  tlsCfg,
			HandshakeTimeout: cfg.WSHandshakeTimeout,
			ReadBufferSize:   cfg.ReadBuf64K,
			WriteBufferSize:  cfg.ReadBuf64K,
		}
		if token != "" {
			dialer.Subprotocols = []string{token}
		}
		if ip != "" {
			dialer.NetDial = func(network, address string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(address)
				// 检查 ip 是否已经包含端口
				if _, _, err := net.SplitHostPort(ip); err == nil {
					// ip 已经包含端口，直接使用
					return net.DialTimeout(network, ip, cfg.DialTimeout)
				}
				// ip 不包含端口，使用 JoinHostPort
				return net.DialTimeout(network, net.JoinHostPort(ip, port), cfg.DialTimeout)
			}
		}

		conn, resp, err := dialer.Dial(dialAddr, nil)
		if err != nil {
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				return nil, fmt.Errorf("认证失败：Token 不匹配或未提供")
			}
			if !fallback && (strings.Contains(err.Error(), "ECH") || strings.Contains(err.Error(), "ech")) && i < retries {
				_ = refreshECH()
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, err
		}
		return conn, nil
	}
	return nil, fmt.Errorf("连接失败")
}
