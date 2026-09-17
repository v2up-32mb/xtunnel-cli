package main

import (
	"github.com/v2up-32mb/xtunnel"
	xsharedconfig "github.com/v2up-32mb/xshared/config"
	xsharedhttpproxy "github.com/v2up-32mb/xshared/httpproxy"
	xsharedsocks5 "github.com/v2up-32mb/xshared/socks5"
)

// startSocks5Listener 以共享 SOCKS5 服务器承接本地监听。
// 数据面由库适配器提供：c.ProxyDialer() 同时具备 TCP 流与 UDP ASSOCIATE 能力；
// 鉴权（RFC1929）与 UDP 端口拦截由 xshared 服务器侧处理。
func startSocks5Listener(addr string, c *xtunnel.Client) error {
	host, user, pass, err := xtunnel.ParseSocks5Auth(addr)
	if err != nil {
		return err
	}
	cfg := &xsharedconfig.Config{ListenAddress: host}
	opts := []xsharedsocks5.Option{}
	if user != "" || pass != "" {
		opts = append(opts, xsharedsocks5.WithUserPassAuth(func(u, p string) bool {
			return xtunnel.AuthEqual(u, user) && xtunnel.AuthEqual(p, pass)
		}))
	}
	if maxSOCKS5Connections > 0 {
		opts = append(opts, xsharedsocks5.WithMaxConns(maxSOCKS5Connections))
	}
	if len(udpBlockedPorts) > 0 {
		opts = append(opts, xsharedsocks5.WithBlockedPorts(udpBlockedPorts))
	}
	if bypassMatcher != nil {
		opts = append(opts, xsharedsocks5.WithBypassMatcher(bypassMatcher))
	}
	return xsharedsocks5.NewServer(cfg, c.ProxyDialer(), opts...).Start()
}

// startHTTPListener 以共享 HTTP 代理服务器承接本地监听（CONNECT 隧道 + 普通代理）。
func startHTTPListener(addr string, c *xtunnel.Client) error {
	host, user, pass, err := xtunnel.ParseSocks5Auth(addr)
	if err != nil {
		return err
	}
	cfg := &xsharedconfig.Config{ListenAddress: host}
	opts := []xsharedhttpproxy.Option{}
	if user != "" || pass != "" {
		opts = append(opts, xsharedhttpproxy.WithUserPassAuth(func(u, p string) bool {
			return xtunnel.AuthEqual(u, user) && xtunnel.AuthEqual(p, pass)
		}))
	}
	if maxSOCKS5Connections > 0 {
		opts = append(opts, xsharedhttpproxy.WithMaxConns(maxSOCKS5Connections))
	}
	if bypassMatcher != nil {
		opts = append(opts, xsharedhttpproxy.WithBypassMatcher(bypassMatcher))
	}
	return xsharedhttpproxy.NewServer(cfg, c.StreamDialer(), opts...).Start()
}
