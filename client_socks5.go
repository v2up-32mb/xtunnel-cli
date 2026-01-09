//go:build client
// +build client

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ProxyConfig SOCKS5 代理配置
type ProxyConfig struct {
	Username, Password, Host string
}

// parseAuthAndAddr 解析认证信息和地址
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

// runSOCKS5Listener 运行 SOCKS5 监听器
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
func handleSOCKS5(c net.Conn, cfgp *ProxyConfig) {
	defer c.Close()

	_ = c.SetDeadline(time.Now().Add(3 * time.Second))

	// VER, NMETHODS
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil || buf[0] != 0x05 {
		return
	}
	methods := make([]byte, buf[1])
	_, _ = io.ReadFull(c, methods)

	// METHOD selection
	if cfgp.Username != "" {
		_, _ = c.Write([]byte{0x05, 0x02}) // username/password
		if err := handleSOCKS5UserPassAuth(c, cfgp); err != nil {
			return
		}
	} else {
		_, _ = c.Write([]byte{0x05, 0x00}) // no auth
	}

	// Request: VER CMD RSV ATYP ...
	head := make([]byte, 4)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}

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

	pb := make([]byte, 2)
	_, _ = io.ReadFull(c, pb)
	port := int(pb[0])<<8 | int(pb[1])

	if head[3] == 0x04 {
		target = fmt.Sprintf("[%s]:%d", target, port)
	} else {
		target = fmt.Sprintf("%s:%d", target, port)
	}

	_ = c.SetDeadline(time.Time{})

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

// handleSOCKS5UserPassAuth 处理 SOCKS5 用户密码认证
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
func handleSOCKS5Connect(c net.Conn, target string) {
	connID := uuid.New().String()

	// reply success (BND.ADDR/BND.PORT ignored)
	_, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	if err != nil {
		_ = c.Close()
		return
	}

	echPool.RegisterAndBroadcastTCP(connID, target, nil, c, "SOCKS5")

	bufPtr := buf32kPool.Get().(*[]byte)
	buf := *bufPtr
	defer buf32kPool.Put(bufPtr)

	defer func() {
		if chID, ok := echPool.GetUplinkChannel(connID); ok {
			_ = echPool.SendCloseDirect(chID, connID)
		} else {
			echPool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
		}
		_ = c.Close()
		echPool.Unregister(connID)
	}()

	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		if chID, ok := echPool.GetUplinkChannel(connID); ok {
			if err := echPool.SendDataDirect(chID, connID, buf[:n]); err != nil {
				log.Printf("[客户端] 发送数据失败: %v, ID:%s", err, shortID(connID))
				return
			}
		} else {
			// uplink 还未确定，使用广播发送
			echPool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, nil, buf[:n]))
		}
	}
}

// UDPAssociation UDP 关联
type UDPAssociation struct {
	connID        string
	tcpConn       net.Conn
	udpListener   *net.UDPConn
	clientUDPAddr *net.UDPAddr
	pool          *ECHPool

	mu        sync.Mutex
	closed    bool
	done      chan bool
	receiving bool
	channelID int
}

// handleSOCKS5UDP 处理 SOCKS5 UDP ASSOCIATE 请求
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

	connID := uuid.New().String()
	assoc := &UDPAssociation{
		connID:      connID,
		tcpConn:     c,
		udpListener: ul,
		pool:        echPool,
		done:        make(chan bool, 5),
		channelID:   -1,
	}
	echPool.RegisterUDP(connID, assoc)

	go assoc.loop()

	// keep TCP alive until closed
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {
			assoc.done <- true
			assoc.Close()
			return
		}
	}
}

// loop UDP 接收循环
func (a *UDPAssociation) loop() {
	bufPtr := buf64kPool.Get().(*[]byte)
	buf := *bufPtr
	defer buf64kPool.Put(bufPtr)

	for {
		n, addr, err := a.udpListener.ReadFromUDP(buf)
		if err != nil {
			a.done <- true
			return
		}

		a.mu.Lock()
		if a.clientUDPAddr == nil {
			a.clientUDPAddr = addr
		} else if a.clientUDPAddr.String() != addr.String() {
			a.mu.Unlock()
			continue
		}
		a.mu.Unlock()

		tgt, data, err := parseSOCKS5UDPPacket(buf[:n])
		if err != nil {
			continue
		}

		// 本地 IP 策略过滤（仅对"已经是 IP 的目标"有意义）
		h, ps, _ := net.SplitHostPort(tgt)
		if ip := net.ParseIP(h); ip != nil {
			if ipStrategy == IPStrategyIPv4Only && ip.To4() == nil {
				continue
			}
			if ipStrategy == IPStrategyIPv6Only && ip.To4() != nil {
				continue
			}
		}

		// UDP 端口拦截（例如拦截 QUIC 443）
		port, _ := strconv.Atoi(ps)
		if udpBlockPorts != nil {
			if _, ok := udpBlockPorts[port]; ok {
				continue
			}
		}

		a.send(tgt, data)
	}
}

// send 发送 UDP 数据
func (a *UDPAssociation) send(target string, data []byte) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	needStart := !a.receiving
	if needStart {
		a.receiving = true
	}
	chID := a.channelID
	a.mu.Unlock()

	if needStart {
		a.pool.StartUDPRace(a.connID, target)
	}

	if chID < 0 {
		if id, ok := a.pool.GetUplinkChannel(a.connID); ok {
			a.mu.Lock()
			a.channelID = id
			chID = id
			a.mu.Unlock()
		} else {
			a.pool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgUDPData, a.connID, nil, data))
			return
		}
	}
	_ = a.pool.SendUDPDataDirect(chID, a.connID, data)
}

// handleUDPResponse 处理 UDP 响应
func (a *UDPAssociation) handleUDPResponse(addrStr string, data []byte) {
	host, portStr, _ := net.SplitHostPort(addrStr)
	port, _ := strconv.Atoi(portStr)

	pkt, err := buildSOCKS5UDPPacket(host, port, data)
	if err != nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clientUDPAddr != nil {
		_, _ = a.udpListener.WriteToUDP(pkt, a.clientUDPAddr)
	}
}

// Close 关闭 UDP 关联
func (a *UDPAssociation) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	closedHadReceiving := a.receiving
	chID := a.channelID
	connID := a.connID
	a.closed = true
	a.mu.Unlock()

	if closedHadReceiving {
		if chID >= 0 {
			a.pool.SendUDPCloseDirect(chID, connID)
		} else {
			a.pool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgUDPClose, connID, nil, nil))
			a.pool.Unregister(connID)
		}
	} else {
		a.pool.Unregister(connID)
	}
	_ = a.udpListener.Close()
}

// parseSOCKS5UDPPacket 解析 SOCKS5 UDP 数据包
func parseSOCKS5UDPPacket(b []byte) (string, []byte, error) {
	// RSV(2)=0, FRAG(1)=0
	if len(b) < 10 || b[2] != 0 {
		return "", nil, errors.New("数据不合法")
	}
	off := 4
	var h string
	switch b[3] {
	case 0x01: // IPv4
		if off+4 > len(b) {
			return "", nil, errors.New("IPv4地址长度过短")
		}
		h = net.IP(b[off : off+4]).String()
		off += 4
	case 0x03: // DOMAIN
		if off+1 > len(b) {
			return "", nil, errors.New("域名长度不足")
		}
		l := int(b[off])
		off++
		if off+l > len(b) {
			return "", nil, errors.New("域名长度不足")
		}
		h = string(b[off : off+l])
		off += l
	case 0x04: // IPv6
		if off+16 > len(b) {
			return "", nil, errors.New("IPv6地址长度过短")
		}
		h = net.IP(b[off : off+16]).String()
		off += 16
	default:
		return "", nil, errors.New("地址类型无效")
	}
	if off+2 > len(b) {
		return "", nil, errors.New("端口字段过短")
	}
	p := int(b[off])<<8 | int(b[off+1])
	off += 2

	t := fmt.Sprintf("%s:%d", h, p)
	if b[3] == 0x04 {
		t = fmt.Sprintf("[%s]:%d", h, p)
	}
	return t, b[off:], nil
}

// buildSOCKS5UDPPacket 构建 SOCKS5 UDP 数据包
func buildSOCKS5UDPPacket(h string, p int, d []byte) ([]byte, error) {
	buf := []byte{0, 0, 0} // RSV(2), FRAG(1)
	ip := net.ParseIP(h)
	if ip4 := ip.To4(); ip4 != nil {
		buf = append(buf, 0x01)
		buf = append(buf, ip4...)
	} else if ip != nil {
		buf = append(buf, 0x04)
		buf = append(buf, ip...)
	} else {
		if len(h) > 255 {
			return nil, errors.New("域名过长")
		}
		buf = append(buf, 0x03, byte(len(h)))
		buf = append(buf, h...)
	}
	buf = append(buf, byte(p>>8), byte(p))
	buf = append(buf, d...)
	return buf, nil
}
