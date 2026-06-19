package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/websocket"
)

// dialWebSocket 建立 WebSocket 连接（支持中转节点）
func (p *clientPool) dialWebSocket(chID int, relayIP string) (*websocket.Conn, error) {
	u, err := url.Parse(p.config.ServerAddr)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(u.Scheme, "wss") {
		return nil, fmt.Errorf("仅支持 wss:// (当前: %s)", u.Scheme)
	}

	dialURL := *u
	q := dialURL.Query()
	// 告诉服务端期望的客户端 ID 和通道 ID
	q.Set("client_id", p.clientID)
	q.Set("ch_id", fmt.Sprintf("%d", chID))
	dialURL.RawQuery = q.Encode()
	dialAddr := dialURL.String()

	serverName := u.Hostname()

	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: p.config.InsecureSkipVerify,
	}

	dialer := websocket.Dialer{
		TLSClientConfig:  tlsCfg,
		HandshakeTimeout: p.config.HandshakeTimeout,
		ReadBufferSize:   p.config.ReadBufferSize,
		WriteBufferSize:  p.config.WriteBufferSize,
	}

	// 设置 Token 认证
	if p.config.Token != "" {
		dialer.Subprotocols = []string{p.config.Token}
	}

	// 设置中转节点
	if relayIP != "" {
		dialer.NetDial = func(network, address string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(address)
			// 检查 relayIP 是否已经包含端口
			if _, _, err := net.SplitHostPort(relayIP); err == nil {
				// relayIP 已经包含端口,直接使用
				return net.DialTimeout(network, relayIP, p.config.DialTimeout)
			}
			// relayIP 不包含端口,使用 JoinHostPort
			return net.DialTimeout(network, net.JoinHostPort(relayIP, port), p.config.DialTimeout)
		}
	}

	conn, resp, err := dialer.Dial(dialAddr, nil)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return nil, fmt.Errorf("认证失败:Token 不匹配或未提供")
		}
		return nil, err
	}
	return conn, nil
}
