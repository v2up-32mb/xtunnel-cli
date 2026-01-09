//go:build client

package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// ======================== SOCKS5 代理 ========================
//
// 实现 RFC 1928 SOCKS5 代理协议，支持：
// - 无认证连接
// - 用户名/密码认证（RFC 1929）
// - CONNECT 命令（TCP 连接）
// - UDP ASSOCIATE 命令（UDP 中继）

// ProxyConfig SOCKS5 代理配置
type ProxyConfig struct {
	Username, Password, Host string // 用户名、密码、监听地址
}

// parseAuthAndAddr 解析认证信息和地址
//
// 支持格式：
//   - host:port（无认证）
//   - user:pass@host:port（带认证）
//
// 返回 host、用户名、密码和可能的错误
func parseAuthAndAddr(full string) (string, string, string, error) {
	u, p, h := "", "", full
	if strings.Contains(full, "@") {
		parts := strings.SplitN(full, "@", 2)
		if len(parts) != 2 {
			return "", "", "", fmt.Errorf("格式错误")
		}
		auth := parts[0]
		if strings.Contains(auth, ":") {
			ap := strings.SplitN(auth, ":", 2)
			u, p = ap[0], ap[1]
		}
		h = parts[1]
	}
	return h, u, p, nil
}

// runSOCKS5Listener 启动 SOCKS5 监听器
//
// 监听 TCP 连接并为每个连接启动一个处理协程。
func runSOCKS5Listener(addr string) {
	h, u, p, err := parseAuthAndAddr(strings.TrimPrefix(addr, "socks5://"))
	if err != nil {
		log.Fatalf("[客户端] SOCKS5地址解析失败: %v", err)
	}
	l, err := net.Listen("tcp", h)
	if err != nil {
		log.Fatalf("[客户端] SOCKS5监听失败: %v", err)
	}
	log.Printf("[客户端] SOCKS5 代理: %s", h)
	cfgp := &ProxyConfig{Username: u, Password: p, Host: h}

	for {
		c, err := l.Accept()
		if err != nil {
			continue
		}
		go handleSOCKS5(c, cfgp)
	}
}

// handleSOCKS5 处理 SOCKS5 连接
//
// 实现 SOCKS5 协议握手和命令处理：
// 1. 版本和方法协商
// 2. 可选的用户名/密码认证
// 3. 解析 CONNECT 或 UDP ASSOCIATE 请求
func handleSOCKS5(c net.Conn, cfgp *ProxyConfig) {
	defer c.Close()

	// 设置初始超时（握手阶段）
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))

	// 1. 版本和方法协商
	// VER, NMETHODS
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil || buf[0] != 0x05 {
		return
	}
	methods := make([]byte, buf[1])
	_, _ = io.ReadFull(c, methods)

	// 2. METHOD selection
	if cfgp.Username != "" {
		_, _ = c.Write([]byte{0x05, 0x02}) // username/password
		if err := handleSOCKS5UserPassAuth(c, cfgp); err != nil {
			return
		}
	} else {
		_, _ = c.Write([]byte{0x05, 0x00}) // no auth
	}

	// 3. Request: VER CMD RSV ATYP ...
	head := make([]byte, 4)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}

	// 解析目标地址
	var target string
	switch head[3] {
	case 0x01: // IPv4
		b := make([]byte, 4)
		_, _ = io.ReadFull(c, b)
		target = net.IP(b).String()
	case 0x03: // DOMAIN
		b := make([]byte, 1)
		_, _ = io.ReadFull(c, b)
		addr := make([]byte, b[0])
		_, _ = io.ReadFull(c, addr)
		target = string(addr)
	case 0x04: // IPv6
		b := make([]byte, 16)
		_, _ = io.ReadFull(c, b)
		target = net.IP(b).String()
	default:
		return
	}

	// 解析端口
	pb := make([]byte, 2)
	_, _ = io.ReadFull(c, pb)
	port := int(pb[0])<<8 | int(pb[1])

	if head[3] == 0x04 {
		target = fmt.Sprintf("[%s]:%d", target, port)
	} else {
		target = fmt.Sprintf("%s:%d", target, port)
	}

	// 清除超时（数据传输阶段）
	_ = c.SetDeadline(time.Time{})

	// 处理命令
	switch head[1] {
	case 0x01: // CONNECT
		handleSOCKS5Connect(c, target)
	case 0x03: // UDP ASSOCIATE
		handleSOCKS5UDP(c, cfgp)
	default:
		// command not supported
		_, _ = c.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
}

// handleSOCKS5UserPassAuth 处理 SOCKS5 用户名密码认证
//
// 实现 RFC 1929 用户名/密码认证子协议。
func handleSOCKS5UserPassAuth(c net.Conn, cfgp *ProxyConfig) error {
	// RFC1929: VER=1, ULEN, UNAME, PLEN, PASSWD
	b := make([]byte, 2)
	_, _ = io.ReadFull(c, b) // VER, ULEN
	u := make([]byte, b[1])
	_, _ = io.ReadFull(c, u)
	_, _ = io.ReadFull(c, b[:1]) // PLEN
	p := make([]byte, b[0])
	_, _ = io.ReadFull(c, p)

	if string(u) == cfgp.Username && string(p) == cfgp.Password {
		_, _ = c.Write([]byte{0x01, 0x00})
		return nil
	}
	_, _ = c.Write([]byte{0x01, 0x01})
	return errors.New("认证失败")
}

// handleSOCKS5Connect 处理 SOCKS5 CONNECT 请求
//
// 处理 TCP CONNECT 命令，建立到目标的双向数据转发。
func handleSOCKS5Connect(c net.Conn, target string) {
	connID := uuid.New().String()

	// reply success (BND.ADDR/BND.PORT ignored)
	_, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	if err != nil {
		_ = c.Close()
		return
	}

	// 注册连接并广播连接请求
	clientPool.RegisterAndBroadcastTCP(connID, target, nil, c, "SOCKS5")

	// 从连接池获取缓冲区
	bufPtr := buf32kPool.Get().(*[]byte)
	buf := *bufPtr
	defer buf32kPool.Put(bufPtr)

	defer func() {
		// 发送关闭消息
		if chID, ok := clientPool.GetUplinkChannel(connID); ok {
			_ = clientPool.SendCloseDirect(chID, connID)
		} else {
			clientPool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
		}
		_ = c.Close()
		clientPool.Unregister(connID)
	}()

	// 数据转发循环
	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		// 如果上行通道已确定，使用单播；否则使用广播
		if chID, ok := clientPool.GetUplinkChannel(connID); ok {
			if err := clientPool.SendDataDirect(chID, connID, buf[:n]); err != nil {
				log.Printf("[客户端] 发送数据失败: %v, ID:%s", err, shortID(connID))
				return
			}
		} else {
			// uplink 还未确定，使用广播发送
			clientPool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, nil, buf[:n]))
		}
	}
}

// handleSOCKS5UDP 处理 SOCKS5 UDP ASSOCIATE 请求
//
// 处理 UDP ASSOCIATE 命令，创建 UDP socket 用于中继 UDP 数据包。
// TCP 连接保持活跃以维持 ASSOCIATE 状态。
func handleSOCKS5UDP(c net.Conn, cfgp *ProxyConfig) {
	host, _, _ := net.SplitHostPort(cfgp.Host)
	uAddr, _ := net.ResolveUDPAddr("udp", net.JoinHostPort(host, "0"))
	ul, err := net.ListenUDP("udp", uAddr)
	if err != nil {
		// general failure
		_, _ = c.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer ul.Close()

	// 构建响应（返回 UDP 监听地址）
	actual := ul.LocalAddr().(*net.UDPAddr)
	resp := []byte{0x05, 0x00, 0x00}
	if ip4 := actual.IP.To4(); ip4 != nil {
		resp = append(resp, 0x01)
		resp = append(resp, ip4...)
	} else {
		resp = append(resp, 0x04)
		resp = append(resp, actual.IP...)
	}
	resp = append(resp, byte(actual.Port>>8), byte(actual.Port))
	_, _ = c.Write(resp)

	// 创建 UDP 关联
	connID := uuid.New().String()
	assoc := &UDPAssociation{
		connID:      connID,
		tcpConn:     c,
		udpListener: ul,
		pool:        clientPool,
		done:        make(chan bool, 5),
		channelID:   -1,
	}
	clientPool.RegisterUDP(connID, assoc)

	// 启动 UDP 接收循环
	go assoc.loop()

	// keep TCP alive until closed（保持 TCP 连接以维持 ASSOCIATE）
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {
			assoc.done <- true
			assoc.Close()
			return
		}
	}
}
