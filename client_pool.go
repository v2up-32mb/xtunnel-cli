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

// WriteJob 写入任务
type WriteJob struct {
	msgType int
	data    []byte
	size    int
}

// ClientConnState 客户端连接状态
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

// ECHPool 客户端连接池
type ECHPool struct {
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

	// RelayNodeManager 中转节点管理器
	relayManager *RelayNodeManager
}

// NewECHPool 创建客户端连接池
func NewECHPool(addr string, n int, _ []string, clientID string) *ECHPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &ECHPool{
		wsServerAddr:     addr,
		connectionNum:    n,
		clientID:         clientID,
		ctx:              ctx,
		cancel:           cancel,
		wsConns:          make([]*websocket.Conn, n),
		writeQueues:      make([]chan WriteJob, n),
		conns:            make(map[string]*ClientConnState),
		globalQueueLimit: 0,
		relayManager:     NewRelayNodeManager(), // 初始化中转节点管理器
	}
	for i := 0; i < n; i++ {
		p.writeQueues[i] = make(chan WriteJob, 4096)
	}
	p.globalQueueLimit = int64(cfg.ReadBuf64K) * 512
	return p
}

// Start 启动所有 WebSocket 连接
func (p *ECHPool) Start() {
	// 启动中转节点管理器
	p.relayManager.Start()

	// 获取最多2个最优节点
	bestNodes := p.relayManager.SelectBestNodes(2)

	if len(bestNodes) > 0 {
		var nodeIPs []string
		for _, node := range bestNodes {
			nodeIPs = append(nodeIPs, node.IP)
			latency := node.Latency.Milliseconds()
			log.Printf("[客户端] 最优中转节点: %s (评分: %.2f, 延迟: %dms)", node.IP, node.Score, latency)
		}
		log.Printf("[客户端] 使用 %d 个最优中转节点，每个节点建立 %d 条连接", len(bestNodes), p.connectionNum)
		log.Printf("[客户端] 共计建立 %d 条 WebSocket 连接", len(bestNodes)*p.connectionNum)

		// 根据最优节点数量重新分配连接池
		total := len(bestNodes) * p.connectionNum
		p.wsConnsMu.Lock()
		p.wsConns = make([]*websocket.Conn, total)
		p.wsConnsMu.Unlock()

		// 重新分配写队列
		newQueues := make([]chan WriteJob, total)
		for i := 0; i < total; i++ {
			newQueues[i] = make(chan WriteJob, 4096)
		}
		p.writeQueues = newQueues

		// 为每个最优节点建立 connectionNum 条连接
		for nodeIdx, node := range bestNodes {
			for j := 0; j < p.connectionNum; j++ {
				chIdx := nodeIdx*p.connectionNum + j
				go p.dialAndServe(chIdx, node.IP)
			}
		}
	} else {
		// 没有中转节点，使用原有逻辑
		log.Printf("[客户端] 未使用中转节点，建立 %d 条连接", p.connectionNum)
		for i := 0; i < len(p.writeQueues); i++ {
			go p.dialAndServe(i, "")
		}
	}
}

// Shutdown 优雅关闭所有 WebSocket 连接
func (p *ECHPool) Shutdown() {
	log.Printf("[客户端] 正在关闭所有连接...")

	// 1. 停止中转节点管理器
	p.relayManager.Stop()

	// 2. 取消 context，通知所有 goroutine 退出
	p.cancel()

	// 3. 关闭所有写队列，停止写入
	for i, q := range p.writeQueues {
		if q != nil {
			close(q)
			p.writeQueues[i] = nil
		}
	}

	// 4. 优雅关闭所有 WebSocket 连接
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
func (p *ECHPool) chIndex(chID int) (int, error) {
	idx := chID - 1
	if idx < 0 || idx >= len(p.writeQueues) {
		return -1, fmt.Errorf("无效的通道ID %d", chID)
	}
	return idx, nil
}

// dialAndServe 连接并服务 WebSocket
func (p *ECHPool) dialAndServe(idx int, ip string) {
	chID := idx + 1
	for {
		// 检查是否需要退出
		select {
		case <-p.ctx.Done():
			log.Printf("[客户端] 通道 %d 已收到退出信号", chID)
			return
		default:
		}

		wsConn, err := dialWebSocketWithECH(p.wsServerAddr, 3, ip, p.clientID, chID)
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

// writeWorker 写入协程
func (p *ECHPool) writeWorker(ctx context.Context, id int, conn *websocket.Conn) {
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
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					log.Printf("[客户端] 通道 %d ping发送失败: %v", id+1, err)
					_ = conn.SetWriteDeadline(time.Time{})
					continue
				}
				_ = conn.SetWriteDeadline(time.Time{})
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

// asyncWriteDirect 异步直接写入指定通道
func (p *ECHPool) asyncWriteDirect(chID int, msgType int, data []byte) error {
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
			log.Printf("[客户端] 通道 %d 写队列满，队列长度: %d", chID, len(p.writeQueues[idx]))
			return fmt.Errorf("通道 %d 缓冲区拥堵", chID)
		}
	}
}

// broadcastWrite 广播写入所有通道
func (p *ECHPool) broadcastWrite(msgType int, data []byte) {
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

// noteUplink 记录上行通道
func (p *ECHPool) noteUplink(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		p.mu.Unlock()
		return
	}
	if st.uplink == 0 {
		st.uplink = chID
	} else if st.uplink != chID {
		// 调试日志：上行通道被覆盖时记录（需要时可解除注释）
		// log.Printf("[客户端] 警告: 连接 %s 的上行通道从 %d 变更为 %d", shortID(connID), st.uplink, chID)
	}
	p.mu.Unlock()
}

// noteLastChannel 记录最后操作的通道
func (p *ECHPool) noteLastChannel(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st != nil {
		st.lastCh = chID
	}
	p.mu.Unlock()
}

// GetUplinkChannel 获取上行通道
func (p *ECHPool) GetUplinkChannel(connID string) (int, bool) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()
	if st == nil || st.uplink == 0 {
		return 0, false
	}
	return st.uplink, true
}

// RegisterAndBroadcastTCP 注册 TCP 连接并广播连接请求
func (p *ECHPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
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

// RegisterUDP 注册 UDP 连接
func (p *ECHPool) RegisterUDP(connID string, assoc *UDPAssociation) {
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

// StartUDPRace 启动 UDP 竞争
func (p *ECHPool) StartUDPRace(connID, target string) {
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

// Unregister 注销连接
func (p *ECHPool) Unregister(connID string) {
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

// selectDownlink 选择下行通道
func (p *ECHPool) selectDownlink(connID string, chID int) (selected bool, chosen int, start time.Time, target string, uplink int, typ string) {
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

// signalConnected 发送连接成功信号
func (p *ECHPool) signalConnected(id string) {
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
func (p *ECHPool) SendDataDirect(chID int, connID string, b []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPData, connID, nil, b))
}

// SendCloseDirect 发送关闭消息到指定通道
func (p *ECHPool) SendCloseDirect(chID int, connID string) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgTCPClose, connID, nil, nil))
}

// SendUDPDataDirect 发送 UDP 数据到指定通道
func (p *ECHPool) SendUDPDataDirect(chID int, connID string, data []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPData, connID, nil, data))
}

// SendUDPCloseDirect 发送 UDP 关闭消息到指定通道
func (p *ECHPool) SendUDPCloseDirect(chID int, connID string) {
	_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, encodeMessage(MsgUDPClose, connID, nil, nil))
	p.Unregister(connID)
}

// cleanupChannel 清理通道
func (p *ECHPool) cleanupChannel(chID int) {
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

// handleChannel 处理通道消息
func (p *ECHPool) handleChannel(chID int, conn *websocket.Conn) {
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
	conn.SetPingHandler(func(m string) error {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		err := p.asyncWriteDirect(chID, websocket.PongMessage, []byte(m))
		if err != nil {
			log.Printf("[客户端] 通道 %d pong发送失败: %v", chID, err)
		}
		return err
	})

	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			if !isNormalCloseError(err) {
				log.Printf("[客户端] 通道 %d 读取消息失败: %v", chID, err)
			} else {
				log.Printf("[客户端] 通道 %d 正常关闭: %v", chID, err)
			}
			return
		}
		// 每次成功读取消息后重置读超时
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
			selected, _, _, target, up, _ := p.selectDownlink(connID, chID)
			if selected {
				p.mu.RLock()
				downlink := 0
				clientAddr := ""
				if st := p.conns[connID]; st != nil {
					downlink = st.downlink
					clientAddr = st.clientAddr
				}
				p.mu.RUnlock()
				if downlink > 0 && target != "" {
					log.Printf("[客户端] %s 访问: %s, 通道: TX %d RX %d, ID:%s",
						clientAddr, target, up, downlink, shortID(connID))
				}
				// 通过 uplink 通道发送 MsgSelectDownlink，meta 中携带下行通道号
				downlinkBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(downlinkBytes, uint32(chID))
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
