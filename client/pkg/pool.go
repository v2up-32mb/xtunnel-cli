package client

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
	"x-tunnel/common"
)

// writeJob 写入任务
type writeJob struct {
	msgType int
	data    []byte
	size    int
}

// clientConnState 客户端连接状态
type clientConnState struct {
	id            string
	target        string
	uplinkChID    int
	downlinkChID  int
	connected     chan bool

	// 内部使用字段
	reqType    string
	tcpConn    net.Conn
	udpAssoc   *udpAssociation
	uplink     int
	downlink   int
	lastCh     int
	start      time.Time
	clientAddr string
	closed     bool
}

// clientPool 客户端连接池
type clientPool struct {
	globalQueueBytes int64
	globalQueueLimit int64
	nextChannel      uint64

	config        *Config
	ctx           context.Context
	cancel        context.CancelFunc
	relayManager  *RelayNodeManager
	echManager    *ECHManager

	wsConnsMu       sync.RWMutex
	wsConns         []*websocket.Conn
	writeQueues     []chan writeJob
	connsWriteMutex []sync.Mutex

	mu    sync.RWMutex
	conns map[string]*clientConnState

	relayCount int
}

// newClientPool 创建新的连接池
func newClientPool(cfg *Config, ctx context.Context, cancel context.CancelFunc) (*clientPool, error) {
	p := &clientPool{
		config:          cfg,
		ctx:             ctx,
		cancel:          cancel,
		echManager:      NewECHManager(cfg),
		relayManager:    NewRelayNodeManager(),
		wsConns:         make([]*websocket.Conn, cfg.Connections),
		writeQueues:     make([]chan writeJob, cfg.Connections),
		connsWriteMutex: make([]sync.Mutex, cfg.Connections),
		conns:           make(map[string]*clientConnState),
		globalQueueLimit: int64(cfg.ReadBufferSize) * 8,
		nextChannel:     1,
	}

	for i := 0; i < cfg.Connections; i++ {
		p.writeQueues[i] = make(chan writeJob, 4096)
	}

	return p, nil
}

// Start 启动连接池
func (p *clientPool) Start(relayNodes []string) {
	// 启动 ECH 管理器（包含定期刷新）
	if p.config.EnableECH {
		go func() {
			if err := p.echManager.Start(); err != nil {
				log.Printf("[客户端] ECH 启动失败: %v", err)
			}
		}()
	}

	// 添加中转节点
	for _, addr := range relayNodes {
		if err := p.relayManager.AddNode(addr, "443"); err != nil {
			log.Printf("[客户端] 添加中转节点失败: %v", err)
		}
	}
	p.relayManager.Start()

	// 保存中转地址数量,用于后续按需申请节点
	p.relayCount = len(relayNodes)

	if p.relayCount > 0 {
		// 初始化时按约定申请指定个数的中转节点
		log.Printf("[客户端] 初始化:按约定申请 %d 个中转节点", p.relayCount)
		bestNodes := p.relayManager.SelectBestNodes(p.relayCount)

		if len(bestNodes) > 0 {
			// 使用申请到的节点建立连接
			log.Printf("[客户端] 初始化成功申请到 %d 个中转节点", len(bestNodes))
			for _, node := range bestNodes {
				latency := node.Latency.Milliseconds()
				log.Printf("[客户端] 中转节点: %s (评分: %.2f, 延迟: %dms)", node.IP, node.Score, latency)
			}
			log.Printf("[客户端] 每个节点建立 %d 条连接", p.config.Connections)
			log.Printf("[客户端] 共计建立 %d 条 WebSocket 连接", len(bestNodes)*p.config.Connections)

			// 根据申请到的节点数量重新分配连接池
			total := len(bestNodes) * p.config.Connections
			p.wsConnsMu.Lock()
			p.wsConns = make([]*websocket.Conn, total)
			p.wsConnsMu.Unlock()

			// 重新分配写队列
			newQueues := make([]chan writeJob, total)
			for i := 0; i < total; i++ {
				newQueues[i] = make(chan writeJob, 4096)
			}
			p.writeQueues = newQueues
			p.connsWriteMutex = make([]sync.Mutex, total)

			// 为每个申请到的节点建立 connectionNum 条连接
			for nodeIdx, node := range bestNodes {
				for j := 0; j < p.config.Connections; j++ {
					chIdx := nodeIdx*p.config.Connections + j
					go p.dialAndServe(chIdx, node.IP)
				}
			}
			return
		}

		// 如果所有节点初始测速都失败
		log.Printf("[客户端] 所有中转节点初始测速失败,直连服务端,建立 %d 条连接", p.config.Connections)
	}

	// 没有指定中转节点或所有节点不可用,直连服务端
	log.Printf("[客户端] 未使用中转节点,直连服务端,建立 %d 条连接", p.config.Connections)
	for i := 0; i < p.config.Connections; i++ {
		go p.dialAndServe(i, "")
	}
}

// Shutdown 关闭连接池
func (p *clientPool) Shutdown() {
	log.Printf("[客户端] 正在关闭所有连接...")

	// 1. 停止 ECH 管理器
	p.echManager.Stop()

	// 2. 停止中转节点管理器
	p.relayManager.Stop()

	// 3. 取消 context,通知所有 goroutine 退出
	p.cancel()

	// 4. 关闭所有写队列,停止写入
	for i, q := range p.writeQueues {
		if q != nil {
			close(q)
			p.writeQueues[i] = nil
		}
	}

	// 5. 优雅关闭所有 WebSocket 连接
	p.wsConnsMu.Lock()
	defer p.wsConnsMu.Unlock()

	var wg sync.WaitGroup
	for i, ws := range p.wsConns {
		if ws != nil {
			wg.Add(1)
			go func(conn *websocket.Conn, chID int, id int) {
				defer wg.Done()
				p.connsWriteMutex[id].Lock()
				// 发送正常的关闭帧
				_ = conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				p.connsWriteMutex[id].Unlock()
				// 等待对方响应或超时
				_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				_ = conn.Close()
				log.Printf("[客户端] 通道 %d 已关闭", chID)
			}(ws, i+1, i)
			p.wsConns[i] = nil
		}
	}
	wg.Wait()

	log.Printf("[客户端] 所有连接已关闭")
}

// chIndex 获取通道索引
func (p *clientPool) chIndex(chID int) (int, error) {
	idx := chID - 1
	if idx < 0 || idx >= len(p.writeQueues) {
		return -1, fmt.Errorf("无效的通道ID %d", chID)
	}
	return idx, nil
}

// dialAndServe 连接并服务 WebSocket
func (p *clientPool) dialAndServe(idx int, ip string) {
	chID := idx + 1
	var relayInfo string
	var lastIP string
	for {
		// 检查是否需要退出
		select {
		case <-p.ctx.Done():
			log.Printf("[客户端] 通道 %d 已收到退出信号", chID)
			return
		default:
		}

		// 如果有中转节点配置,在重连时申请新节点
		if p.relayCount > 0 && lastIP != "" {
			healthyIPs := p.relayManager.GetHealthyRelayIPs()
			newNode := p.relayManager.SelectNodeExcluding(healthyIPs)
			if newNode != nil {
				log.Printf("[客户端] 通道 %d 重连:申请新中转节点 %s (评分: %.2f, 延迟: %dms)",
					chID, newNode.IP, newNode.Score, newNode.Latency.Milliseconds())
				ip = newNode.IP
				relayInfo = fmt.Sprintf(" [中转: %s]", ip)
			} else {
				log.Printf("[客户端] 通道 %d 重连:无可用的健康中转节点,使用原有节点", chID)
			}
		}

		wsConn, err := p.dialWebSocket(chID, ip)
		if err != nil {
			if relayInfo == "" && ip != "" {
				relayInfo = fmt.Sprintf(" [中转: %s]", ip)
			}
			log.Printf("[客户端] 通道 %d%s 连接失败: %v", chID, relayInfo, err)
			// 检查是否需要退出（避免在重连延迟时阻塞）
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(3 * time.Second):
				continue
			}
		}

		if ip != "" {
			relayInfo = fmt.Sprintf(" [中转: %s]", ip)
			lastIP = ip
		}
		log.Printf("[客户端] 通道 %d%s 已连接", chID, relayInfo)
		p.wsConnsMu.Lock()
		p.wsConns[idx] = wsConn
		p.wsConnsMu.Unlock()

		go p.writeWorker(idx, wsConn)
		p.handleChannel(chID, wsConn)

		_ = wsConn.Close()

		p.wsConnsMu.Lock()
		p.wsConns[idx] = nil
		p.wsConnsMu.Unlock()
		p.cleanupChannel(chID)

		log.Printf("[客户端] 通道 %d%s 断开,重连中...", chID, relayInfo)
		time.Sleep(p.config.ReconnectDelay)
	}
}

// writeWorker 写入协程
func (p *clientPool) writeWorker(id int, conn *websocket.Conn) {
	queue := p.writeQueues[id]
	ticker := time.NewTicker(p.config.PingInterval)
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

	var pending *writeJob
	for {
		var job writeJob
		if pending != nil {
			job = *pending
			pending = nil
		} else {
			select {
			case <-p.ctx.Done():
				return
			case j, ok := <-queue:
				if !ok {
					// 队列已关闭,正常退出
					return
				}
				job = j
			case <-ticker.C:
				p.connsWriteMutex[id].Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
				if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					log.Printf("[客户端] 通道 %d ping发送失败: %v", id+1, err)
					p.connsWriteMutex[id].Unlock()
					_ = conn.Close()
					return
				}
				_ = conn.SetWriteDeadline(time.Time{})
				p.connsWriteMutex[id].Unlock()
				continue
			}
		}

		atomic.AddInt64(&p.globalQueueBytes, int64(-job.size))

		// 非二进制消息直接写
		if job.msgType != websocket.BinaryMessage {
			p.connsWriteMutex[id].Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
			if err := conn.WriteMessage(job.msgType, job.data); err != nil {
				p.connsWriteMutex[id].Unlock()
				_ = conn.Close()
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
			p.connsWriteMutex[id].Unlock()
			continue
		}

		// TCPData 聚合:减少帧数
		t, connID, meta, payload, err := common.DecodeMessage(job.data)
		if err != nil || t != common.MsgTCPData {
			p.connsWriteMutex[id].Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
			if err := conn.WriteMessage(job.msgType, job.data); err != nil {
				p.connsWriteMutex[id].Unlock()
				_ = conn.Close()
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
			p.connsWriteMutex[id].Unlock()
			continue
		}

		maxAgg := p.config.ReadBufferSize * 4
		total := len(payload)
		parts := [][]byte{payload}

		for {
			select {
			case next, ok := <-queue:
				if !ok {
					// 队列已关闭,正常退出
					goto writeAgg
				}
				atomic.AddInt64(&p.globalQueueBytes, int64(-next.size))
				if next.msgType != websocket.BinaryMessage {
					pending = &next
					goto writeAgg
				}
				tt, cid, mm, pl, e := common.DecodeMessage(next.data)
				if e != nil || tt != common.MsgTCPData || cid != connID || len(mm) != 0 {
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
			for _, pt := range parts {
				copy(merged[off:], pt)
				off += len(pt)
			}
		}

		p.connsWriteMutex[id].Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
		if err := conn.WriteMessage(websocket.BinaryMessage, common.EncodeMessage(common.MsgTCPData, connID, meta, merged)); err != nil {
			p.connsWriteMutex[id].Unlock()
			_ = conn.Close()
			return
		}
		_ = conn.SetWriteDeadline(time.Time{})
		p.connsWriteMutex[id].Unlock()
	}
}

// asyncWriteDirect 异步直接写入指定通道
func (p *clientPool) asyncWriteDirect(chID int, msgType int, data []byte) error {
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
	case p.writeQueues[idx] <- writeJob{msgType: msgType, data: data, size: int(size)}:
		return nil
	default:
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case p.writeQueues[idx] <- writeJob{msgType: msgType, data: data, size: int(size)}:
			return nil
		case <-timer.C:
			atomic.AddInt64(&p.globalQueueBytes, -size)
			log.Printf("[客户端] 通道 %d 写队列满,队列长度: %d", chID, len(p.writeQueues[idx]))
			return fmt.Errorf("通道 %d 缓冲区拥堵", chID)
		}
	}
}

// broadcastWrite 广播写入所有通道
func (p *clientPool) broadcastWrite(msgType int, data []byte) {
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
	// 没有可用连接:仍丢入某个通道队列,等待其重连后发送（队列可能积压/丢弃由限额控制）
	idx := int(atomic.AddUint64(&p.nextChannel, 1)) % len(p.writeQueues)
	_ = p.asyncWriteDirect(idx+1, msgType, data)
}

// noteUplink 记录上行通道
func (p *clientPool) noteUplink(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		p.mu.Unlock()
		return
	}
	if st.uplink == 0 {
		st.uplink = chID
	} else if st.uplink != chID {
		// 调试日志:上行通道被覆盖时记录（需要时可解除注释）
		// log.Printf("[客户端] 警告: 连接 %s 的上行通道从 %d 变更为 %d", common.ShortID(connID), st.uplink, chID)
	}
	p.mu.Unlock()
}

// noteLastChannel 记录最后操作的通道
func (p *clientPool) noteLastChannel(connID string, chID int) {
	p.mu.Lock()
	st := p.conns[connID]
	if st != nil {
		st.lastCh = chID
	}
	p.mu.Unlock()
}

// GetUplinkChannel 获取上行通道
func (p *clientPool) GetUplinkChannel(connID string) (int, bool) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()
	if st == nil || st.uplink == 0 {
		return 0, false
	}
	return st.uplink, true
}

// RegisterAndBroadcastTCP 注册 TCP 连接并广播连接请求
func (p *clientPool) RegisterAndBroadcastTCP(connID, target string, first []byte, tcpConn net.Conn, reqType string) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &clientConnState{}
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
	meta[0] = byte(p.config.IPStrategy)
	copy(meta[1:], target)

	msg := common.EncodeMessage(common.MsgTCPConnect, connID, meta, first)
	p.broadcastWrite(websocket.BinaryMessage, msg)
}

// RegisterUDP 注册 UDP 连接
func (p *clientPool) RegisterUDP(connID string, assoc *udpAssociation) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &clientConnState{}
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
func (p *clientPool) StartUDPRace(connID, target string) {
	p.mu.Lock()
	st := p.conns[connID]
	if st == nil {
		st = &clientConnState{}
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
	meta[0] = byte(p.config.IPStrategy)
	copy(meta[1:], target)

	p.broadcastWrite(websocket.BinaryMessage, common.EncodeMessage(common.MsgUDPConnect, connID, meta, nil))
}

// Unregister 注销连接
func (p *clientPool) Unregister(connID string) {
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
		client, typ, target, u, d, common.ShortID(connID))

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
func (p *clientPool) selectDownlink(connID string, chID int) (selected bool, chosen int, start time.Time, target string, uplink int, typ string) {
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
func (p *clientPool) signalConnected(id string) {
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
func (p *clientPool) SendDataDirect(chID int, connID string, b []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, common.EncodeMessage(common.MsgTCPData, connID, nil, b))
}

// SendCloseDirect 发送关闭消息到指定通道
func (p *clientPool) SendCloseDirect(chID int, connID string) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, common.EncodeMessage(common.MsgTCPClose, connID, nil, nil))
}

// SendUDPDataDirect 发送 UDP 数据到指定通道
func (p *clientPool) SendUDPDataDirect(chID int, connID string, data []byte) error {
	return p.asyncWriteDirect(chID, websocket.BinaryMessage, common.EncodeMessage(common.MsgUDPData, connID, nil, data))
}

// SendUDPCloseDirect 发送 UDP 关闭消息到指定通道
func (p *clientPool) SendUDPCloseDirect(chID int, connID string) {
	_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, common.EncodeMessage(common.MsgUDPClose, connID, nil, nil))
	p.Unregister(connID)
}

// cleanupChannel 清理通道
func (p *clientPool) cleanupChannel(chID int) {
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
func (p *clientPool) handleChannel(chID int, conn *websocket.Conn) {
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
	conn.SetPingHandler(func(m string) error {
		_ = conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
		err := p.asyncWriteDirect(chID, websocket.PongMessage, []byte(m))
		if err != nil {
			log.Printf("[客户端] 通道 %d pong发送失败: %v", chID, err)
		}
		// pong 发送失败不影响 ping/pong 循环,总是返回 nil
		return nil
	})

	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			if !common.IsNormalCloseError(err) {
				log.Printf("[客户端] 通道 %d 读取消息失败: %v", chID, err)
			} else {
				log.Printf("[客户端] 通道 %d 正常关闭: %v", chID, err)
			}
			return
		}
		// 每次成功读取消息后重置读超时
		_ = conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))

		if mt != websocket.BinaryMessage {
			continue
		}
		mtype, connID, meta, payload, err := common.DecodeMessage(msg)
		if err != nil {
			continue
		}

		p.noteLastChannel(connID, chID)

		switch mtype {
		case common.MsgSelectUplink:
			// 从 meta 中解析服务端选择的上行通道ID
			var uplinkChID int
			if len(meta) >= 4 {
				uplinkChID = int(binary.BigEndian.Uint32(meta[0:4]))
			} else {
				// 兼容旧版本:使用当前处理通道
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
						clientAddr, target, up, downlink, common.ShortID(connID))
				}
				// 通过 uplink 通道发送 MsgSelectDownlink,meta 中携带下行通道号
				downlinkBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(downlinkBytes, uint32(chID))
				_ = p.asyncWriteDirect(uplinkChID, websocket.BinaryMessage, common.EncodeMessage(common.MsgSelectDownlink, connID, downlinkBytes, nil))
			}

		case common.MsgConnStatus:
			if len(meta) < 1 {
				continue
			}
			if common.ConnStatus(meta[0]) == common.StatusOK {
				// 不做阻塞等待,这里只作为"连接建立"的信号
				p.signalConnected(connID)
			} else {
				p.Unregister(connID)
			}

		case common.MsgTCPData:
			// 下行数据:只处理来自已选中下行通道的数据
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

		case common.MsgTCPClose:
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

		case common.MsgUDPData:
			selected, chosen, start, target, up, typ := p.selectDownlink(connID, chID)
			if selected {
				downlinkBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(downlinkBytes, uint32(chID))
				_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, common.EncodeMessage(common.MsgSelectDownlink, connID, downlinkBytes, nil))
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
						client, typ, target, up, chID, common.ShortID(connID), ms)
				}
			}
			if chosen != chID {
				continue
			}
			p.mu.RLock()
			var assoc *udpAssociation
			if st := p.conns[connID]; st != nil {
				assoc = st.udpAssoc
			}
			p.mu.RUnlock()
			if assoc != nil {
				assoc.handleUDPResponse(string(meta), payload)
			}

		case common.MsgUDPClose:
			p.noteUplink(connID, chID)
			p.mu.RLock()
			var assoc *udpAssociation
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

// Stats 返回统计信息
func (p *clientPool) Stats() *Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return &Stats{
		Connections:    len(p.conns),
		ActiveChannels: 0,
		RelayNodes:     0,
	}
}
