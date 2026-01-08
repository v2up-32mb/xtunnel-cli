//go:build client
// +build client

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type GlobalConfig struct {
	DialTimeout        time.Duration
	WSHandshakeTimeout time.Duration
	WSWriteTimeout     time.Duration
	WSReadTimeout      time.Duration
	PingInterval       time.Duration
	ReconnectDelay     time.Duration

	ReadBuf32K int
	ReadBuf64K int
}

var cfg = GlobalConfig{
	DialTimeout:        3 * time.Second,
	WSHandshakeTimeout: 5 * time.Second,
	WSWriteTimeout:     5 * time.Second,
	WSReadTimeout:      10 * time.Second,
	PingInterval:       3 * time.Second,
	ReconnectDelay:     1 * time.Second,
	ReadBuf32K:         32 * 1024,
	ReadBuf64K:         64 * 1024,
}

var buf32kPool = sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }}
var buf64kPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// ======================== 客户端参数 ========================

var (
	listenAddr       string
	forwardAddr      string
	ipAddr           string
	udpBlockPortsStr string
	token            string
	insecure         bool
	connectionNum    int
	ips              string

	clientPool *ClientPool

	clientID      string
	udpBlockPorts map[int]struct{}
	ipStrategy    byte
)

const (
	// IP 策略常量定义见 protocol.go
)

func init() {
	flag.StringVar(&listenAddr, "l", "", "监听地址 (仅支持 socks5://，支持多个用逗号分隔)\n示例:\n  socks5://[user:pass@]0.0.0.0:1080")
	flag.StringVar(&forwardAddr, "f", "", "服务端地址 (仅客户端模式，必须是 wss://host:port/path)")
	flag.StringVar(&ipAddr, "ip", "", "指定连接 wss 的目标 IP（将 wss 主机名定向到该 IP 连接），多个IP用逗号分隔")
	flag.StringVar(&udpBlockPortsStr, "block", "443", "客户端拦截 UDP 端口列表，逗号分隔，如 443,8443")
	flag.BoolVar(&insecure, "insecure", false, "wss 模式忽略证书校验")
	flag.StringVar(&token, "token", "", "身份验证令牌（WebSocket Subprotocol）")
	flag.IntVar(&connectionNum, "n", 3, "每个IP建立的WebSocket连接数量")
	flag.StringVar(&ips, "ips", "", "服务端解析目标地址的IP偏好\n 4: 仅IPv4\n 6: 仅IPv6\n 4,6: IPv4优先\n 6,4: IPv6优先")
}

func main() {
	flag.Parse()

	if listenAddr == "" || forwardAddr == "" {
		flag.Usage()
		return
	}

	// 仅支持 socks5:// 监听
	listeners := strings.Split(listenAddr, ",")
	for _, l := range listeners {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if !strings.HasPrefix(l, "socks5://") {
			log.Fatalf("[客户端] 已移除除 SOCKS5 外的监听支持：非法监听地址 %q", l)
		}
	}

	forwardURL, err := url.Parse(forwardAddr)
	if err != nil {
		log.Fatalf("[客户端] 无效的服务地址: %v", err)
	}
	if !strings.EqualFold(forwardURL.Scheme, "wss") {
		log.Fatalf("[客户端] 安全要求：仅支持 wss:// 协议 (当前: %s)", forwardURL.Scheme)
	}

	ipStrategy = parseIPStrategy(ips)
	if ips != "" {
		log.Printf("[客户端] IP 访问策略: %s (code: %d)", ips, ipStrategy)
	}

	var targetIPs []string
	if ipAddr != "" {
		for _, p := range strings.Split(ipAddr, ",") {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				targetIPs = append(targetIPs, trimmed)
			}
		}
	}

	if udpBlockPortsStr != "" {
		udpBlockPorts = make(map[int]struct{})
		for _, p := range strings.Split(udpBlockPortsStr, ",") {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			var port int
			_, _ = fmt.Sscanf(pp, "%d", &port)
			if port > 0 && port < 65536 {
				udpBlockPorts[port] = struct{}{}
			}
		}
	}

	clientID = uuid.NewString()
	log.Printf("[客户端] 客户端ID: %s", clientID)

	clientPool = NewClientPool(forwardAddr, connectionNum, targetIPs, clientID)
	clientPool.Start()

	// 监听退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	for _, listenerRule := range listeners {
		rule := strings.TrimSpace(listenerRule)
		if rule == "" {
			continue
		}
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			runSOCKS5Listener(r)
		}(rule)
	}

	// 等待退出信号或监听器退出
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-sigChan:
		log.Printf("[客户端] 收到退出信号，正在优雅关闭...")
		clientPool.Shutdown()
		os.Exit(0)
	case <-done:
		// 所有监听器已退出
	}
}

// ======================== TLS 配置 ========================

func buildTLSConfig(serverName string) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: insecure,
	}, nil
}

// ======================== 多通道客户端池 ========================

type WriteJob struct {
	msgType int
	data    []byte
	size    int
}

type ClientConnState struct {
	reqType    string
	tcpConn    net.Conn
	udpAssoc   *UDPAssociation
	uplink     int
	downlink   int
	lastCh     int
	start      time.Time
	target     string
	connected  chan bool
	clientAddr string
	closed     bool
}

type ClientPool struct {
	globalQueueBytes int64
	globalQueueLimit int64
	nextChannel      uint64

	wsServerAddr  string
	connectionNum int
	targetIPs     []string
	clientID      string

	ctx    context.Context
	cancel context.CancelFunc

	wsConnsMu   sync.RWMutex
	wsConns     []*websocket.Conn
	writeQueues []chan WriteJob

	mu    sync.RWMutex
	conns map[string]*ClientConnState
}

func NewClientPool(addr string, n int, ips []string, clientID string) *ClientPool {
	total := n
	if len(ips) > 0 {
		total = len(ips) * n
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &ClientPool{
		wsServerAddr:     addr,
		connectionNum:    n,
		targetIPs:        ips,
		clientID:         clientID,
		ctx:              ctx,
		cancel:           cancel,
		wsConns:          make([]*websocket.Conn, total),
		writeQueues:      make([]chan WriteJob, total),
		conns:            make(map[string]*ClientConnState),
		globalQueueLimit: 0,
	}
	for i := 0; i < total; i++ {
		p.writeQueues[i] = make(chan WriteJob, 4096)
	}
	p.globalQueueLimit = int64(cfg.ReadBuf64K) * 512
	return p
}

func (p *ClientPool) Start() {
	for i := 0; i < len(p.writeQueues); i++ {
		ip := ""
		if len(p.targetIPs) > 0 {
			if idx := i / p.connectionNum; idx < len(p.targetIPs) {
				ip = p.targetIPs[idx]
			}
		}
		go p.dialAndServe(i, ip)
	}
}

// Shutdown 优雅关闭所有 WebSocket 连接
func (p *ClientPool) Shutdown() {
	log.Printf("[客户端] 正在关闭所有连接...")

	// 1. 取消 context，通知所有 goroutine 退出
	p.cancel()

	// 2. 关闭所有写队列，停止写入
	for i, q := range p.writeQueues {
		if q != nil {
			close(q)
			p.writeQueues[i] = nil
		}
	}

	// 3. 优雅关闭所有 WebSocket 连接
	p.wsConnsMu.Lock()
	defer p.wsConnsMu.Unlock()

	var wg sync.WaitGroup
	for i, ws := range p.wsConns {
		if ws != nil {
			wg.Add(1)
			go func(conn *websocket.Conn, chID int) {
				defer wg.Done()
				// 发送正常的关闭帧
				_ = conn.WriteMessage(websocket.CloseMessage,
				 websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				// 等待对方响应或超时
				_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				_ = conn.Close()
				log.Printf("[客户端] 通道 %d 已关闭", chID)
			}(ws, i+1)
			p.wsConns[i] = nil
		}
	}
	wg.Wait()

	log.Printf("[客户端] 所有连接已关闭")
}

func (p *ClientPool) chIndex(chID int) (int, error) {
	idx := chID - 1
	if idx < 0 || idx >= len(p.writeQueues) {
		return -1, fmt.Errorf("无效的通道ID %d", chID)
	}
	return idx, nil
}

func (p *ClientPool) dialAndServe(idx int, ip string) {
	chID := idx + 1
	for {
		// 检查是否需要退出
		select {
		case <-p.ctx.Done():
			log.Printf("[客户端] 通道 %d 已收到退出信号", chID)
			return
		default:
		}

		wsConn, err := dialWebSocket(p.wsServerAddr, ip, p.clientID, chID)
		if err != nil {
			log.Printf("[客户端] 通道 %d (IP:%s) 连接失败: %v", chID, ip, err)
			// 检查是否需要退出（避免在重连延迟时阻塞）
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(3 * time.Second):
				continue
			}
		}
		log.Printf("[客户端] 通道 %d 已连接", chID)
		p.wsConnsMu.Lock()
		p.wsConns[idx] = wsConn
		p.wsConnsMu.Unlock()

		ctx, cancel := context.WithCancel(p.ctx)
		go p.writeWorker(ctx, idx, wsConn)
		p.handleChannel(chID, wsConn)
		cancel()
		_ = wsConn.Close()

		p.wsConnsMu.Lock()
		p.wsConns[idx] = nil
		p.wsConnsMu.Unlock()
		p.cleanupChannel(chID)

		log.Printf("[客户端] 通道 %d 断开，重连中...", chID)
		time.Sleep(cfg.ReconnectDelay)
	}
}

func (p *ClientPool) writeWorker(ctx context.Context, id int, conn *websocket.Conn) {
	queue := p.writeQueues[id]
	ticker := time.NewTicker(cfg.PingInterval)
	defer ticker.Stop()

	// 退出时尽量回收 globalQueueBytes
	defer func() {
		if queue != nil {
			for {
				select {
				case j, ok := <-queue:
					if !ok {
						return
					}
					atomic.AddInt64(&p.globalQueueBytes, int64(-j.size))
				default:
					return
				}
			}
		}
	}()

	var pending *WriteJob
	for {
		var job WriteJob
		if pending != nil {
			job = *pending
			pending = nil
		} else {
			select {
			case <-ctx.Done():
				return
			case j, ok := <-queue:
				if !ok {
					// 队列已关闭
					return
				}
				job = j
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					_ = conn.Close()
					return
				}
				continue
			}
		}

		atomic.AddInt64(&p.globalQueueBytes, int64(-job.size))

		// 非二进制消息直接写
		if job.msgType != websocket.BinaryMessage {
			_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
			if err := conn.WriteMessage(job.msgType, job.data); err != nil {
				_ = conn.Close()
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
			continue
		}

		// TCPData 聚合：减少帧数
		t, connID, meta, payload, err := decodeMessage(job.data)
		if err != nil || t != MsgTCPData {
			_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
			if err := conn.WriteMessage(job.msgType, job.data); err != nil {
				_ = conn.Close()
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
			continue
		}

		maxAgg := cfg.ReadBuf64K * 4
		total := len(payload)
		parts := [][]byte{payload}

		for {
			select {
			case next, ok := <-queue:
				if !ok {
					// 队列已关闭
					goto writeAgg
				}
				atomic.AddInt64(&p.globalQueueBytes, int64(-next.size))
				if next.msgType != websocket.BinaryMessage {
					pending = &next
					goto writeAgg
				}
				tt, cid, mm, pl, e := decodeMessage(next.data)
				if e != nil || tt != MsgTCPData || cid != connID || len(mm) != 0 {
					pending = &next
					goto writeAgg
				}
				if total+len(pl) > maxAgg {
					pending = &next
					goto writeAgg
				}
				parts = append(parts, pl)
				total += len(pl)
			default:
				goto writeAgg
			}
		}

	writeAgg:
		var merged []byte
		if len(parts) == 1 {
			merged = parts[0]
		} else {
			merged = make([]byte, total)
			off := 0
			for _, p0 := range parts {
				copy(merged[off:], p0)
				off += len(p0)
			}
		}

		_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
		if err := conn.WriteMessage(websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, meta, merged)); err != nil {
			_ = conn.Close()
			return
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func (p *ClientPool) asyncWriteDirect(chID int, msgType int, data []byte) error {
	idx, err := p.chIndex(chID)
	if err != nil {
		return err
	}

	size := int64(len(data))
	if atomic.AddInt64(&p.globalQueueBytes, size) > p.globalQueueLimit {
		atomic.AddInt64(&p.globalQueueBytes, -size)
		return fmt.Errorf("全局写队列超限")
	}

	select {
	case p.writeQueues[idx] <- WriteJob{msgType: msgType, data: data, size: int(size)}:
		return nil
	default:
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case p.writeQueues[idx] <- WriteJob{msgType: msgType, data: data, size: int(size)}:
			return nil
		case <-timer.C:
			atomic.AddInt64(&p.globalQueueBytes, -size)
			return fmt.Errorf("通道 %d 缓冲区拥堵", chID)
		}
	}
}

func (p *ClientPool) broadcastWrite(msgType int, data []byte) {
	p.wsConnsMu.RLock()
	sent := false
	for i, c := range p.wsConns {
		if c == nil {
			continue
		}
		_ = p.asyncWriteDirect(i+1, msgType, data)
		sent = true
	}
	p.wsConnsMu.RUnlock()

	if sent {
		return
	}
	// 没有可用连接：仍丢入某个通道队列，等待其重连后发送（队列可能积压/丢弃由限额控制）
	idx := int(atomic.AddUint64(&p.nextChannel, 1)) % len(p.writeQueues)
	_ = p.asyncWriteDirect(idx+1, msgType, data)
}

func (p *ClientPool) noteUplink(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		p.mu.Unlock()
		return
	}
	if st.uplink == 0 {
		st.uplink = chID
	}
	p.mu.Unlock()
}

func (p *ClientPool) noteLastChannel(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st != nil {
		st.lastCh = chID
	}
	p.mu.Unlock()
}

func (p *ClientPool) GetUplinkChannel(connID string) (int, bool) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()
	if st == nil || st.uplink == 0 {
		return 0, false
	}
	return st.uplink, true
}

func (p *ClientPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &ClientConnState{}
		p.conns[connID] = st
	}
	st.tcpConn = tcpConn
	st.target = target
	st.connected = make(chan bool, 1)
	st.start = time.Now()
	if reqType != "" {
		st.reqType = reqType
	}
	if tcpConn != nil {
		if ra := tcpConn.RemoteAddr(); ra != nil {
			st.clientAddr = ra.String()
		}
	}
	st.uplink = 0
	st.downlink = 0
	st.lastCh = 0
	st.closed = false
	p.mu.Unlock()

	meta := make([]byte, 1+len(target))
	meta[0] = ipStrategy
	copy(meta[1:], target)

	msg := encodeMessage(MsgTCPConnect, connID, meta, first)
	p.broadcastWrite(websocket.BinaryMessage, msg)
}

func (p *ClientPool) RegisterUDP(connID string, assoc *UDPAssociation) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &ClientConnState{}
		p.conns[connID] = st
	}
	st.udpAssoc = assoc
	if st.connected == nil {
		st.connected = make(chan bool, 1)
	}
	if st.reqType == "" {
		st.reqType = "SOCKS5 UDP"
	}
	if assoc != nil && assoc.tcpConn != nil {
		if ra := assoc.tcpConn.RemoteAddr(); ra != nil {
			st.clientAddr = ra.String()
		}
	}
	p.mu.Unlock()
}

func (p *ClientPool) StartUDPRace(connID, target string) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &ClientConnState{}
		p.conns[connID] = st
	}
	st.target = target
	st.start = time.Now()
	st.reqType = "SOCKS5 UDP"
	st.uplink = 0
	st.downlink = 0
	st.lastCh = 0
	p.mu.Unlock()

	meta := make([]byte, 1+len(target))
	meta[0] = ipStrategy
	copy(meta[1:], target)

	p.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgUDPConnect, connID, meta, nil))
}

func (p *ClientPool) Unregister(connID string) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		p.mu.Unlock()
		return
	}
	if st.closed {
		p.mu.Unlock()
		return
	}
	st.closed = true

	target := st.target
	up, down := st.uplink, st.downlink
	if up == 0 && st.lastCh > 0 {
		up = st.lastCh
	}
	if down == 0 && st.lastCh > 0 {
		down = st.lastCh
	}

	u := "-"
	d := "-"
	if up > 0 {
		u = fmt.Sprintf("%d", up)
	}
	if down > 0 {
		d = fmt.Sprintf("%d", down)
	}

	client := "-"
	typ := st.reqType
	if typ == "" {
		typ = "请求"
	}
	if st.clientAddr != "" {
		client = st.clientAddr
	}
	if target == "" {
		target = "-"
	}

	log.Printf("[客户端] %s %s 访问: %s, 通道: TX %s RX %s, ID:%s, 已关闭",
		client, typ, target, u, d, shortID(connID))

	if st.tcpConn != nil {
		_ = st.tcpConn.Close()
	}
	if st.udpAssoc != nil {
		st.udpAssoc.Close()
	}
	delete(p.conns, connID)
	p.mu.Unlock()
}

func (p *ClientPool) selectDownlink(connID string, chID int) (selected bool, chosen int, start time.Time, target string, uplink int, typ string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.conns[connID]
	if st == nil || st.target == "" {
		return
	}
	if st.downlink > 0 {
		chosen = st.downlink
		selected = false
	} else {
		st.downlink = chID
		chosen = chID
		selected = true
		start = st.start
	}
	target = st.target
	uplink = -1
	if st.uplink > 0 {
		uplink = st.uplink
	}
	typ = st.reqType
	return
}

func (p *ClientPool) handleChannel(chID int, conn *websocket.Conn) {
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
	conn.SetPingHandler(func(m string) error {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		return p.asyncWriteDirect(chID, websocket.PongMessage, []byte(m))
	})

	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			if !isNormalCloseError(err) {
				log.Printf("[客户端] 通道 %d 异常: %v", chID, err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))

		if mt != websocket.BinaryMessage {
			continue
		}
		mtype, connID, meta, payload, err := decodeMessage(msg)
		if err != nil {
			continue
		}

		p.noteLastChannel(connID, chID)

		switch mtype {
		case MsgSelectUplink:
			// 从 meta 中解析服务端选择的上行通道ID
			var uplinkChID int
			if len(meta) >= 4 {
				uplinkChID = int(binary.BigEndian.Uint32(meta[0:4]))
			} else {
				// 兼容旧版本：使用当前处理通道
				uplinkChID = chID
			}
			p.noteUplink(connID, uplinkChID)

			// 选择当前通道作为下行通道（最快收到 MsgSelectUplink 的获胜）
			selected, chosen, _, target, up, _ := p.selectDownlink(connID, chID)
			if selected {
				p.mu.RLock()
				clientAddr := ""
				if st := p.conns[connID]; st != nil {
					clientAddr = st.clientAddr
				}
				p.mu.RUnlock()
				if chosen > 0 && target != "" {
					log.Printf("[客户端] %s 访问: %s, 通道: TX %d RX %d, ID:%s",
						clientAddr, target, up, chosen, shortID(connID))
				}
				// 通过 uplink 通道发送 MsgSelectDownlink，meta 中携带下行通道号
				downlinkBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(downlinkBytes, uint32(chosen))
				_ = p.asyncWriteDirect(uplinkChID, websocket.BinaryMessage, encodeMessage(MsgSelectDownlink, connID, downlinkBytes, nil))
			}

		case MsgConnStatus:
			if len(meta) < 1 {
				continue
			}
			if ConnStatus(meta[0]) == StatusOK {
				// 不做阻塞等待，这里只作为"连接建立"的信号
				p.signalConnected(connID)
			} else {
				p.Unregister(connID)
			}

		case MsgTCPData:
			// 下行数据：只处理来自已选中下行通道的数据
			_, chosen, _, _, _, _ := p.selectDownlink(connID, chID)
			if chosen != chID {
				continue
			}
			p.mu.RLock()
			var c net.Conn
			if st := p.conns[connID]; st != nil {
				c = st.tcpConn
			}
			p.mu.RUnlock()
			if c != nil {
				_ = c.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := c.Write(payload); err != nil {
					_ = p.SendCloseDirect(chID, connID)
					_ = c.Close()
				}
				_ = c.SetWriteDeadline(time.Time{})
			} else {
				_ = p.SendCloseDirect(chID, connID)
			}

		case MsgTCPClose:
			p.noteUplink(connID, chID)
			p.mu.RLock()
			var c net.Conn
			if st := p.conns[connID]; st != nil {
				c = st.tcpConn
			}
			p.mu.RUnlock()
			if c != nil {
				_ = c.Close()
			}
			p.Unregister(connID)

		case MsgUDPData:
			selected, chosen, start, target, up, typ := p.selectDownlink(connID, chID)
			if selected {
				_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgSelectDownlink, connID, nil, nil))
				if !start.IsZero() && up > 0 {
					if typ == "" {
						typ = "SOCKS5 UDP"
					}
					client := "-"
					p.mu.RLock()
					if st := p.conns[connID]; st != nil && st.clientAddr != "" {
						client = st.clientAddr
					}
					p.mu.RUnlock()
					ms := float64(time.Since(start)) / float64(time.Millisecond)
					log.Printf("[客户端] %s %s 访问: %s, 通道: TX %d RX %d, ID:%s, 延迟 %.1f ms",
						client, typ, target, up, chID, shortID(connID), ms)
				}
			}
			if chosen != chID {
				continue
			}
			p.mu.RLock()
			var assoc *UDPAssociation
			if st := p.conns[connID]; st != nil {
				assoc = st.udpAssoc
			}
			p.mu.RUnlock()
			if assoc != nil {
				assoc.handleUDPResponse(string(meta), payload)
			}

		case MsgUDPClose:
			p.noteUplink(connID, chID)
			p.mu.RLock()
			var assoc *UDPAssociation
			if st := p.conns[connID]; st != nil {
				assoc = st.udpAssoc
			}
			p.mu.RUnlock()
			if assoc != nil {
				assoc.Close()
			} else {
				p.Unregister(connID)
			}
		}
	}
}

func (p *ClientPool) signalConnected(id string) {
	p.mu.RLock()
	st := p.conns[id]
	var ch chan bool
	if st != nil {
		ch = st.connected
	}
	p.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- true:
		default:
		}
	}
}

func (p *ClientPool) SendDataDirect(chID int, connID string, b []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, nil, b))
}

func (p *ClientPool) SendCloseDirect(chID int, connID string) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
}

func (p *ClientPool) SendUDPDataDirect(chID int, connID string, data []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPData, connID, nil, data))
}

func (p *ClientPool) SendUDPCloseDirect(chID int, connID string) {
	_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPClose, connID, nil, nil))
	p.Unregister(connID)
}

func (p *ClientPool) cleanupChannel(chID int) {
	p.mu.Lock()
	var toClose []string
	for id, st := range p.conns {
		if st.uplink == chID || st.downlink == chID {
			toClose = append(toClose, id)
		}
	}
	p.mu.Unlock()

	for _, id := range toClose {
		p.mu.RLock()
		st := p.conns[id]
		p.mu.RUnlock()
		if st == nil {
			continue
		}
		if st.tcpConn != nil {
			_ = st.tcpConn.Close()
		}
		if st.udpAssoc != nil {
			st.udpAssoc.Close()
		}
		p.Unregister(id)
	}
}

// dialWebSocket：客户端仅支持 wss://
func dialWebSocket(addr string, ip string, clientID string, chID int) (*websocket.Conn, error) {
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
	tlsCfg, err := buildTLSConfig(serverName)
	if err != nil {
		return nil, err
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
			return net.DialTimeout(network, net.JoinHostPort(ip, port), cfg.DialTimeout)
		}
	}

	conn, resp, err := dialer.Dial(dialAddr, nil)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return nil, fmt.Errorf("认证失败：Token 不匹配或未提供")
		}
		return nil, err
	}
	return conn, nil
}

// ======================== SOCKS5 代理（客户端监听） ========================

type ProxyConfig struct {
	Username, Password, Host string
}

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

func handleSOCKS5Connect(c net.Conn, target string) {
	connID := uuid.New().String()

	// reply success (BND.ADDR/BND.PORT ignored)
	_, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	if err != nil {
		_ = c.Close()
		return
	}

	clientPool.RegisterAndBroadcastTCP(connID, target, nil, c, "SOCKS5")

	bufPtr := buf32kPool.Get().(*[]byte)
	buf := *bufPtr
	defer buf32kPool.Put(bufPtr)

	defer func() {
		if chID, ok := clientPool.GetUplinkChannel(connID); ok {
			_ = clientPool.SendCloseDirect(chID, connID)
		} else {
			clientPool.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
		}
		_ = c.Close()
		clientPool.Unregister(connID)
	}()

	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
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

type UDPAssociation struct {
	connID        string
	tcpConn       net.Conn
	udpListener   *net.UDPConn
	clientUDPAddr *net.UDPAddr
	pool          *ClientPool

	mu        sync.Mutex
	closed    bool
	done      chan bool
	receiving bool
	channelID int
}

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
		pool:        clientPool,
		done:        make(chan bool, 5),
		channelID:   -1,
	}
	clientPool.RegisterUDP(connID, assoc)

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

		// 本地 IP 策略过滤（仅对“已经是 IP 的目标”有意义）
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
		var prt int
		_, _ = fmt.Sscanf(ps, "%d", &prt)
		if udpBlockPorts != nil {
			if _, ok := udpBlockPorts[prt]; ok {
				continue
			}
		}

		a.send(tgt, data)
	}
}

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