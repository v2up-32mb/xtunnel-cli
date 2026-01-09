//go:build client

package main

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/gorilla/websocket"
)

// ======================== UDP 关联 ========================
//
// UDPAssociation 管理 SOCKS5 UDP ASSOCIATE 状态。
//
// SOCKS5 UDP 数据包格式（RFC 1928）：
// +-----+------+------+------+--------+--------+
// | RSV | FRAG | ATYP | DST.ADDR | DST.PORT | DATA   |
// |(2B) | (1B) | (1B) | (var)    | (2B)     | (var)  |
// +-----+------+------+------+--------+--------+
//
// 响应时 DST.ADDR/DST.PORT 为源地址/端口

// UDPAssociation UDP 关联
type UDPAssociation struct {
	connID        string           // 连接唯一标识
	tcpConn       net.Conn         // TCP 控制连接
	udpListener   *net.UDPConn     // UDP 监听 socket
	clientUDPAddr *net.UDPAddr     // 客户端 UDP 地址
	pool          *ClientPool      // 连接池引用

	mu        sync.Mutex       // 保护以下字段的锁
	closed    bool             // 是否已关闭
	done      chan bool        // 完成信号
	receiving bool             // 是否正在接收
	channelID int              // 上行通道 ID
}

// loop UDP 接收循环
//
// 持续接收来自客户端的 UDP 数据包，解析后转发到服务端。
// 实现客户端地址验证和端口拦截。
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

		// 验证客户端地址（只接受来自同一客户端的数据包）
		a.mu.Lock()
		if a.clientUDPAddr == nil {
			a.clientUDPAddr = addr
		} else if a.clientUDPAddr.String() != addr.String() {
			a.mu.Unlock()
			continue
		}
		a.mu.Unlock()

		// 解析 SOCKS5 UDP 数据包
		tgt, data, err := parseSOCKS5UDPPacket(buf[:n])
		if err != nil {
			continue
		}

		// 本地 IP 策略过滤（仅对"已经是 IP 的目标"有意义）
		h, ps, _ := net.SplitHostPort(tgt)
		if ip := net.ParseIP(h); ip != nil {
			if cfg.IPStrategy == IPStrategyIPv4Only && ip.To4() == nil {
				continue
			}
			if cfg.IPStrategy == IPStrategyIPv6Only && ip.To4() != nil {
				continue
			}
		}

		// UDP 端口拦截（例如拦截 QUIC 443）
		var prt int
		_, _ = fmt.Sscanf(ps, "%d", &prt)
		if cfg.UDPBlockPorts != nil {
			if _, ok := cfg.UDPBlockPorts[prt]; ok {
				continue
			}
		}

		a.send(tgt, data)
	}
}

// send 发送 UDP 数据
//
// 首次发送时启动通道竞争，后续发送使用已确定的通道。
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
		// 尝试获取上行通道
		if id, ok := a.pool.GetUplinkChannel(a.connID); ok {
			a.mu.Lock()
			a.channelID = id
			chID = id
			a.mu.Unlock()
		} else {
			// uplink 还未确定，使用广播发送
			a.pool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgUDPData, a.connID, nil, data))
			return
		}
	}
	_ = a.pool.SendUDPDataDirect(chID, a.connID, data)
}

// handleUDPResponse 处理 UDP 响应
//
// 将服务端的 UDP 响应封装为 SOCKS5 UDP 数据包发回客户端。
func (a *UDPAssociation) handleUDPResponse(addrStr string, data []byte) {
	host, portStr, _ := net.SplitHostPort(addrStr)
	port := 0
	_, _ = fmt.Sscanf(portStr, "%d", &port)

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
//
// 发送 UDP 关闭消息并清理资源。
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
//
// 按照 RFC 1928 格式解析 UDP 数据包，提取目标地址和负载数据。
//
// 返回目标地址（host:port 格式）和负载数据
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

	// 格式化目标地址
	t := fmt.Sprintf("%s:%d", h, p)
	if b[3] == 0x04 {
		t = fmt.Sprintf("[%s]:%d", h, p)
	}
	return t, b[off:], nil
}

// buildSOCKS5UDPPacket 构建 SOCKS5 UDP 数据包
//
// 按照 RFC 1928 格式构建 UDP 数据包，用于发送响应给客户端。
//
// 参数:
//   - h: 主机地址（IP 或域名）
//   - p: 端口号
//   - d: 负载数据
//
// 返回编码后的数据包
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
		// 域名
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
