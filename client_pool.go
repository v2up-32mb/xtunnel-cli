//go:build client

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ======================== 客户端连接池 ========================
//
// ClientPool 管理多个 WebSocket 通道连接，实现：
// - 多通道并发传输（提高吞吐量和可靠性）
// - 通道竞争选择（选择最快的通道作为上行/下行通道）
// - TCP 数据聚合（减少帧数）
// - 优雅关闭

// WriteJob 写入任务
type WriteJob struct {
	msgType int    // WebSocket 消息类型
	data    []byte // 待写入数据
	size    int    // 数据大小（用于队列流量控制）
}

// ClientConnState 客户端连接状态
type ClientConnState struct {
	reqType    string      // 请求类型（如 "SOCKS5"、"SOCKS5 UDP"）
	tcpConn    net.Conn    // TCP 连接（TCP 请求时使用）
	udpAssoc   *UDPAssociation // UDP 关联（UDP 请求时使用）
	uplink     int         // 上行通道 ID（0 表示未确定）
	downlink   int         // 下行通道 ID（0 表示未确定）
	lastCh     int         // 最后使用的通道 ID
	start      time.Time   // 连接开始时间
	target     string      // 目标地址
	connected  chan bool   // 连接成功信号
	clientAddr string      // 客户端地址
	closed     bool        // 是否已关闭
}

// ClientPool 客户端连接池
type ClientPool struct {
	// 全局队列流量控制
	globalQueueBytes int64 // 当前全局队列字节数
	globalQueueLimit int64 // 全局队列字节数限制
	nextChannel      uint64 // 下一个通道索引（用于轮询）

	// 服务器配置
	wsServerAddr  string   // WebSocket 服务器地址
	connectionNum int      // 每个 IP 的连接数
	targetIPs     []string // 目标 IP 列表（可选）
	clientID      string   // 客户端唯一标识

	// 上下文控制（用于优雅关闭）
	ctx    context.Context
	cancel context.CancelFunc

	// WebSocket 连接和写队列
	wsConnsMu   sync.RWMutex         // WebSocket 连接锁
	wsConns     []*websocket.Conn    // WebSocket 连接列表
	writeQueues []chan WriteJob       // 写队列列表

	// 连接状态管理
	mu    sync.RWMutex              // 连接状态锁
	conns map[string]*ClientConnState // 连接 ID -> 连接状态
}

// NewClientPool 创建客户端连接池
//
// 参数:
//   - addr: WebSocket 服务器地址
//   - n: 每个 IP 的连接数
//   - ips: 目标 IP 列表（可选，用于多 IP 连接）
//   - clientID: 客户端唯一标识
//
// 返回初始化好的连接池
func NewClientPool(addr string, n int, ips []string, clientID string) *ClientPool {
	total := n
	// 如果指定了多个 IP，总连接数为 IP 数量 × 每个连接数
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
	// 初始化写队列
	for i := 0; i < total; i++ {
		p.writeQueues[i] = make(chan WriteJob, 4096)
	}
	// 设置全局队列限制为 64KB × 512 = 32MB
	p.globalQueueLimit = int64(cfg.ReadBuf64K) * 512
	return p
}

// Start 启动所有通道连接
//
// 为每个通道启动一个 goroutine 进行连接和服务
func (p *ClientPool) Start() {
	for i := 0; i < len(p.writeQueues); i++ {
		// 计算当前通道对应的 IP（如果指定了多个 IP）
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
//
// 关闭流程：
// 1. 取消 context，通知所有 goroutine 退出
// 2. 关闭所有写队列
// 3. 发送 WebSocket Close Frame (1000) 并关闭连接
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

// chIndex 获取通道索引
//
// 将通道 ID（从 1 开始）转换为数组索引（从 0 开始）
func (p *ClientPool) chIndex(chID int) (int, error) {
	idx := chID - 1
	if idx < 0 || idx >= len(p.writeQueues) {
		return -1, fmt.Errorf("无效的通道ID %d", chID)
	}
	return idx, nil
}

// asyncWriteDirect 异步直接写入指定通道
//
// 将数据写入指定通道的写队列，带流量控制。
// 如果队列满，会等待 100ms，超时则返回错误。
//
// 参数:
//   - chID: 通道 ID
//   - msgType: WebSocket 消息类型
//   - data: 待写入数据
//
// 返回可能的错误（如队列超限、缓冲区拥堵）
func (p *ClientPool) asyncWriteDirect(chID int, msgType int, data []byte) error {
	idx, err := p.chIndex(chID)
	if err != nil {
		return err
	}

	size := int64(len(data))
	// 全局队列流量控制
	if atomic.AddInt64(&p.globalQueueBytes, size) > p.globalQueueLimit {
		atomic.AddInt64(&p.globalQueueBytes, -size)
		return fmt.Errorf("全局写队列超限")
	}

	// 尝试写入队列
	select {
	case p.writeQueues[idx] <- WriteJob{msgType: msgType, data: data, size: int(size)}:
		return nil
	default:
		// 队列满，等待 100ms
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

// broadcastWrite 广播写入所有通道
//
// 将数据写入所有已连接的通道。如果没有可用连接，
// 则写入一个随机通道队列（等待其重连后发送）。
//
// 用于上行通道未确定时的数据发送。
//
// 参数:
//   - msgType: WebSocket 消息类型
//   - data: 待写入数据
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
	// 没有可用连接：仍丢入某个通道队列，等待其重连后发送
	idx := int(atomic.AddUint64(&p.nextChannel, 1)) % len(p.writeQueues)
	_ = p.asyncWriteDirect(idx+1, msgType, data)
}

// noteUplink 记录上行通道
//
// 记录服务端选择的上行通道 ID。只记录第一次设置的值。
func (p *ClientPool) noteUplink(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		p.mu.Unlock()
		return
	}
	// 只记录第一次设置的值（避免被覆盖）
	if st.uplink == 0 {
		st.uplink = chID
	}
	p.mu.Unlock()
}

// noteLastChannel 记录最后使用的通道
func (p *ClientPool) noteLastChannel(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st != nil {
		st.lastCh = chID
	}
	p.mu.Unlock()
}

// GetUplinkChannel 获取上行通道
//
// 返回指定连接的上行通道 ID 和是否有效。
func (p *ClientPool) GetUplinkChannel(connID string) (int, bool) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()
	if st == nil || st.uplink == 0 {
		return 0, false
	}
	return st.uplink, true
}

// RegisterAndBroadcastTCP 注册 TCP 连接并广播连接请求
//
// 注册一个新的 TCP 连接，并广播 MsgTCPConnect 到所有通道。
// 服务端会通过竞争选择最快的通道作为上行通道。
//
// 参数:
//   - connID: 连接唯一标识
//   - target: 目标地址
//   - first: 可选的首批数据（如 TLS ClientHello）
//   - tcpConn: TCP 连接
//   - reqType: 请求类型（如 "SOCKS5"）
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

	// 构建元数据：[IPStrategy][TargetAddress]
	meta := make([]byte, 1+len(target))
	meta[0] = cfg.IPStrategy
	copy(meta[1:], target)

	msg := encodeMessage(MsgTCPConnect, connID, meta, first)
	p.broadcastWrite(websocket.BinaryMessage, msg)
}

// RegisterUDP 注册 UDP 连接
//
// 注册一个新的 UDP 关联（不立即发送连接请求）。
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

// StartUDPRace 开始 UDP 竞争
//
// 广播 MsgUDPConnect 到所有通道，服务端会通过竞争选择最快的通道。
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
	meta[0] = cfg.IPStrategy
	copy(meta[1:], target)

	p.broadcastWrite(websocket.BinaryMessage, encodeMessage(MsgUDPConnect, connID, meta, nil))
}

// Unregister 注销连接
//
// 关闭并清理连接资源，输出访问日志。
// 使用 closed 标志防止重复注销。
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
	// 如果上下行通道未确定，使用最后使用的通道
	if up == 0 && st.lastCh > 0 {
		up = st.lastCh
	}
	if down == 0 && st.lastCh > 0 {
		down = st.lastCh
	}

	// 格式化通道 ID 用于日志输出
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

	// 关闭底层连接
	if st.tcpConn != nil {
		_ = st.tcpConn.Close()
	}
	if st.udpAssoc != nil {
		st.udpAssoc.Close()
	}
	delete(p.conns, connID)
	p.mu.Unlock()
}

// selectDownlink 选择下行通道
//
// 客户端收到 MsgSelectUplink 时，最快响应的通道被选为下行通道。
// 返回值：
//   - selected: 是否是新选择的（false 表示已选择过）
//   - chosen: 选中的下行通道 ID
//   - start: 连接开始时间（用于计算延迟）
//   - target: 目标地址
//   - uplink: 上行通道 ID
//   - typ: 请求类型
func (p *ClientPool) selectDownlink(connID string, chID int) (selected bool, chosen int, start time.Time, target string, uplink int, typ string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.conns[connID]
	if st == nil || st.target == "" {
		return
	}
	// 如果已经选择过，直接返回
	if st.downlink > 0 {
		chosen = st.downlink
		selected = false
	} else {
		// 第一次选择，记录当前通道为下行通道
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

// signalConnected 发送连接成功信号
//
// 通知连接已建立（用于同步等待）
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

// SendDataDirect 发送数据到指定通道
func (p *ClientPool) SendDataDirect(chID int, connID string, b []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, nil, b))
}

// SendCloseDirect 发送关闭消息到指定通道
func (p *ClientPool) SendCloseDirect(chID int, connID string) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
}

// SendUDPDataDirect 发送 UDP 数据到指定通道
func (p *ClientPool) SendUDPDataDirect(chID int, connID string, data []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPData, connID, nil, data))
}

// SendUDPCloseDirect 发送 UDP 关闭消息到指定通道
func (p *ClientPool) SendUDPCloseDirect(chID int, connID string) {
	_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPClose, connID, nil, nil))
	p.Unregister(connID)
}

// cleanupChannel 清理通道
//
// 当通道断开时，关闭所有使用该通道作为上下行的连接。
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

// dialAndServe 建立连接并服务
//
// 为单个通道建立 WebSocket 连接，并在断开时自动重连。
// 启动一个 writeWorker 处理写入，当前 goroutine 处理读取。
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

		// 启动写入工作协程
		ctx, cancel := context.WithCancel(p.ctx)
		go p.writeWorker(ctx, idx, wsConn)
		// 当前协程处理读取
		p.handleChannel(chID, wsConn)
		cancel()
		_ = wsConn.Close()

		// 清理连接状态
		p.wsConnsMu.Lock()
		p.wsConns[idx] = nil
		p.wsConnsMu.Unlock()
		p.cleanupChannel(chID)

		log.Printf("[客户端] 通道 %d 断开，重连中...", chID)
		time.Sleep(cfg.ReconnectDelay)
	}
}

// writeWorker 写入工作协程
//
// 从写队列中读取任务并写入 WebSocket 连接。
// 实现：
// - 定时发送 Ping 心跳
// - TCP 数据聚合（合并连续的 TCPData 消息减少帧数）
// - 响应 context 取消信号
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
					return
				}
				job = j
			case <-ticker.C:
				// 发送 Ping 心跳
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

		// 尝试聚合更多的 TCPData 消息
		maxAgg := cfg.ReadBuf64K * 4
		total := len(payload)
		parts := [][]byte{payload}

		for {
			select {
			case next, ok := <-queue:
				if !ok {
					goto writeAgg
				}
				atomic.AddInt64(&p.globalQueueBytes, int64(-next.size))
				// 非二进制消息或不同连接，停止聚合
				if next.msgType != websocket.BinaryMessage {
					pending = &next
					goto writeAgg
				}
				tt, cid, mm, pl, e := decodeMessage(next.data)
				// 不同消息类型、不同连接或有元数据，停止聚合
				if e != nil || tt != MsgTCPData || cid != connID || len(mm) != 0 {
					pending = &next
					goto writeAgg
				}
				// 超过最大聚合大小，停止聚合
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
		// 合并聚合的数据
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

// handleChannel 处理通道消息
//
// 处理从服务端接收的所有消息，包括：
// - Ping/Pong 心跳
// - 通道选择（上行/下行）
// - TCP/UDP 数据传输
// - 连接状态通知
func (p *ClientPool) handleChannel(chID int, conn *websocket.Conn) {
	// 设置 Pong 处理器（更新读取超时）
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
	// 设置 Ping 处理器（回复 Pong）
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
			// 服务端选择上行通道
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
			// 连接状态通知
			if len(meta) < 1 {
				continue
			}
			if ConnStatus(meta[0]) == StatusOK {
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
			// TCP 连接关闭
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
			// UDP 数据
			selected, chosen, start, target, up, typ := p.selectDownlink(connID, chID)
			if selected {
				downlinkBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(downlinkBytes, uint32(chID))
				_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgSelectDownlink, connID, downlinkBytes, nil))
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
			// UDP 连接关闭
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
