//go:build server
// +build server

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// ======================== 服务端连接状态 ========================

type ServerConnState struct {
	connID       string
	clientID     string
	target       string
	targetConn   net.Conn
	targetUDP    *net.UDPConn
	uplinkChID   int
	downlinkChID int
	ipStrategy   byte
	isUDP        bool
	clientAddr   string
	connected    bool
	closed       bool // 防止重复注销
	mu           sync.RWMutex
	pendingData  [][]byte // 连接建立前到达的数据缓存
}

// writeTask 写入任务
type writeTask struct {
	msgType int
	data    []byte
}

// ServerWSConn WebSocket 连接
type ServerWSConn struct {
	ws        *websocket.Conn
	chID      int
	clientID  string
	pool      *ServerPool
	mu        sync.Mutex
	closed    bool
	writeChan chan writeTask
}

func (wsConn *ServerWSConn) start() {
	go wsConn.writeLoop()
}

func (wsConn *ServerWSConn) readLoop() {
	defer func() {
		if !wsConn.closed {
			wsConn.close()
		}
	}()

	wsConn.ws.SetPongHandler(func(string) error {
		return wsConn.ws.SetReadDeadline(time.Now().Add(serverCfg.WSReadTimeout))
	})
	wsConn.ws.SetReadDeadline(time.Now().Add(serverCfg.WSReadTimeout))
	wsConn.ws.SetPingHandler(func(m string) error {
		wsConn.ws.SetReadDeadline(time.Now().Add(serverCfg.WSReadTimeout))
		_ = wsConn.asyncWrite(websocket.PongMessage, []byte(m))
		// pong 发送失败不影响 ping/pong 循环，总是返回 nil
		return nil
	})

	for {
		mt, msg, err := wsConn.ws.ReadMessage()
		if err != nil {
			if !isNormalCloseError(err) {
				log.Printf("[服务端] 通道 %d 读取消息失败: %v", wsConn.chID, err)
			} else {
				log.Printf("[服务端] 通道 %d 正常关闭: %v", wsConn.chID, err)
			}
			return
		}
		// 每次成功读取消息后重置读超时
		wsConn.ws.SetReadDeadline(time.Now().Add(serverCfg.WSReadTimeout))

		if mt != websocket.BinaryMessage {
			continue
		}

		msgType, connID, meta, payload, err := decodeMessage(msg)
		if err != nil {
			continue
		}

		wsConn.pool.handleMessage(wsConn.chID, msgType, connID, meta, payload)
	}
}

func (wsConn *ServerWSConn) writeLoop() {
	ticker := time.NewTicker(serverCfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case task, ok := <-wsConn.writeChan:
			if !ok {
				return
			}
			if err := wsConn.writeDirect(task.msgType, task.data); err != nil {
				log.Printf("[服务端] 通道 %d 写消息失败: %v", wsConn.chID, err)
				wsConn.close()
				return
			}
		case <-ticker.C:
			if err := wsConn.writeDirect(websocket.PingMessage, []byte{}); err != nil {
				log.Printf("[服务端] 通道 %d ping发送失败: %v", wsConn.chID, err)
				continue
			}
		}
	}
}

func (wsConn *ServerWSConn) asyncWrite(msgType int, data []byte) error {
	wsConn.mu.Lock()
	if wsConn.closed {
		wsConn.mu.Unlock()
		return nil
	}
	select {
	case wsConn.writeChan <- writeTask{msgType: msgType, data: data}:
		wsConn.mu.Unlock()
		return nil
	default:
		wsConn.mu.Unlock()
		return fmt.Errorf("写队列满")
	}
}

func (wsConn *ServerWSConn) writeDirect(msgType int, data []byte) error {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()

	if wsConn.closed {
		return nil
	}

	if msgType != websocket.BinaryMessage {
		_ = wsConn.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
			return err
		}
		_ = wsConn.ws.SetWriteDeadline(time.Time{})
		return nil
	}

	_ = wsConn.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
		return err
	}
	_ = wsConn.ws.SetWriteDeadline(time.Time{})
	return nil
}

func (wsConn *ServerWSConn) close() {
	wsConn.mu.Lock()
	if wsConn.closed {
		wsConn.mu.Unlock()
		return
	}
	wsConn.closed = true
	wsConn.mu.Unlock()

	_ = wsConn.ws.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	_ = wsConn.ws.Close()

	// 安全关闭channel
	if wsConn.writeChan != nil {
		select {
		case <-wsConn.writeChan:
			// channel已经关闭
		default:
			close(wsConn.writeChan)
		}
	}

	wsConn.pool.cleanupChannel(wsConn.chID)
}

// ======================== 服务端连接池 ========================

type ServerPool struct {
	token string
	mu    sync.RWMutex

	// 连接状态映射
	conns map[string]*ServerConnState

	// WebSocket 连接
	wsConns []*ServerWSConn
	chConns map[int]*ServerWSConn

	nextChID int
}

func NewServerPool(token string) *ServerPool {
	return &ServerPool{
		token:    token,
		conns:    make(map[string]*ServerConnState),
		wsConns:  make([]*ServerWSConn, 0),
		chConns:  make(map[int]*ServerWSConn),
		nextChID: 1,
	}
}

func (p *ServerPool) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 验证 Token
	requestedProtocols := websocket.Subprotocols(r)
	if len(requestedProtocols) == 0 || requestedProtocols[0] != p.token {
		log.Printf("[服务端] 认证失败: 来自 %s", r.RemoteAddr)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// 解析 client_id
	queryParams, _ := url.ParseQuery(r.URL.RawQuery)
	clientID := queryParams.Get("client_id")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	// 解析客户端期望的通道 ID
	var chID int
	if chIDStr := queryParams.Get("ch_id"); chIDStr != "" {
		_, err := fmt.Sscanf(chIDStr, "%d", &chID)
		if err != nil || chID <= 0 {
			log.Printf("[服务端] 无效的 ch_id 参数: %s", chIDStr)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	} else {
		// 如果客户端没有提供 ch_id，服务端自动分配
		p.mu.Lock()
		chID = p.nextChID
		p.nextChID++
		p.mu.Unlock()
	}

	// 升级为 WebSocket
	upgrader.Subprotocols = []string{p.token}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[服务端] WebSocket 升级失败: %v", err)
		return
	}

	wsConn := &ServerWSConn{
		ws:        ws,
		chID:      chID,
		clientID:  clientID,
		pool:      p,
		writeChan: make(chan writeTask, 1024),
	}

	// 存储 WebSocket 连接
	p.mu.Lock()
	// 扩展切片
	for chID > len(p.wsConns) {
		p.wsConns = append(p.wsConns, nil)
	}
	if chID == len(p.wsConns) {
		p.wsConns = append(p.wsConns, wsConn)
	} else {
		p.wsConns[chID-1] = wsConn
	}
	p.chConns[chID] = wsConn
	p.mu.Unlock()

	log.Printf("[服务端] 通道 %d 已连接, 客户端: %s", chID, clientID)

	// 启动写入协程
	wsConn.start()

	// 启动读取循环
	wsConn.readLoop()

	log.Printf("[服务端] 通道 %d 已断开", chID)
}

func (p *ServerPool) handleMessage(chID int, msgType MessageType, connID string, meta, payload []byte) {
	switch msgType {
	case MsgTCPConnect:
		p.handleTCPConnect(chID, connID, meta)

	case MsgTCPData:
		p.handleTCPData(chID, connID, payload)

	case MsgSelectDownlink:
		p.handleSelectDownlink(chID, connID, meta)

	case MsgTCPClose:
		p.handleTCPClose(chID, connID)

	case MsgUDPConnect:
		p.handleUDPConnect(chID, connID, meta)

	case MsgUDPData:
		p.handleUDPData(chID, connID, meta, payload)

	case MsgUDPClose:
		p.handleUDPClose(chID, connID)
	}
}

func (p *ServerPool) sendDownlink(connID string, msgType MessageType, meta, payload []byte) error {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil {
		return fmt.Errorf("连接不存在")
	}

	st.mu.RLock()
	downlink := st.downlinkChID
	st.mu.RUnlock()

	if downlink > 0 {
		// 已选择下行通道：单播
		return p.sendToChannel(downlink, websocket.BinaryMessage, encodeMessage(msgType, connID, meta, payload))
	}

	// 未选择：广播
	return p.broadcastWrite(websocket.BinaryMessage, encodeMessage(msgType, connID, meta, payload))
}

func (p *ServerPool) broadcastWrite(msgType int, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var activeConns []*ServerWSConn
	for _, wsConn := range p.wsConns {
		if wsConn != nil && !wsConn.closed {
			activeConns = append(activeConns, wsConn)
		}
	}

	if len(activeConns) == 0 {
		return fmt.Errorf("无可用通道")
	}

	// 发送到所有活跃通道，忽略写队列满错误
	for _, wsConn := range activeConns {
		_ = wsConn.asyncWrite(msgType, data)
	}
	return nil
}

func (p *ServerPool) sendToChannel(chID int, msgType int, data []byte) error {
	p.mu.RLock()
	wsConn := p.chConns[chID]
	p.mu.RUnlock()

	if wsConn == nil || wsConn.closed {
		return fmt.Errorf("通道 %d 不可用", chID)
	}

	_ = wsConn.asyncWrite(msgType, data)
	return nil
}

func (p *ServerPool) cleanupChannel(chID int) {
	p.mu.Lock()

	var wsConn *ServerWSConn
	if chID <= len(p.wsConns) {
		wsConn = p.wsConns[chID-1]
		p.wsConns[chID-1] = nil
	}
	delete(p.chConns, chID)

	// 清理使用此通道的连接
	var toClose []string
	for connID, st := range p.conns {
		st.mu.RLock()
		useCh := st.uplinkChID == chID || st.downlinkChID == chID
		st.mu.RUnlock()

		if useCh {
			toClose = append(toClose, connID)
		}
	}
	p.mu.Unlock()

	for _, connID := range toClose {
		p.unregisterConn(connID)
	}

	if wsConn != nil {
		wsConn.mu.Lock()
		if !wsConn.closed {
			wsConn.closed = true
			// 安全关闭channel
			if wsConn.writeChan != nil {
				select {
				case <-wsConn.writeChan:
					// channel已经关闭
				default:
					close(wsConn.writeChan)
				}
			}
			wsConn.ws.Close()
		}
		wsConn.mu.Unlock()
	}
}

// ======================== TCP 处理 ========================

func (p *ServerPool) handleTCPConnect(chID int, connID string, meta []byte) {
	if len(meta) < 1 {
		p.sendDownlink(connID, MsgConnStatus, []byte{byte(StatusERR)}, nil)
		return
	}

	ipStrategy := meta[0]
	target := string(meta[1:])

	// 第一个到达的通道占用连接，后续的丢弃
	p.mu.Lock()
	st, exists := p.conns[connID]
	if !exists {
		// 第一个到达的通道：创建状态并占用
		st = &ServerConnState{
			connID:     connID,
			target:     target,
			uplinkChID: chID,
			ipStrategy: ipStrategy,
			isUDP:      false,
			connected:  false,
		}
		p.conns[connID] = st
		p.mu.Unlock()

		// 获取客户端地址
		p.mu.RLock()
		wsConn := p.chConns[chID]
		p.mu.RUnlock()
		if wsConn != nil {
			st.clientID = wsConn.clientID
			st.clientAddr = wsConn.clientID
		}

		// 发送 MsgSelectUplink（广播），携带上行通道ID
		uplinkChIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
		_ = p.sendDownlink(connID, MsgSelectUplink, uplinkChIDBytes, nil)

		log.Printf("[服务端] %s 访问: %s, 通道: TX %d, ID:%s", st.clientAddr, target, chID, shortID(connID))

		// 异步连接目标服务器
		go p.connectTarget(st)

	} else {
		// 后续通道：丢弃
		p.mu.Unlock()
		// 已有其他通道处理此连接，静默丢弃
	}
}

func (p *ServerPool) connectTarget(st *ServerConnState) {
	// IP 策略解析
	resolvedTarget := resolveWithStrategy(st.target, st.ipStrategy)

	// 连接到目标
	conn, err := net.DialTimeout("tcp", resolvedTarget, 10*time.Second)
	if err != nil {
		log.Printf("[服务端] 连接目标失败 %s: %v", st.target, err)
		p.sendDownlink(st.connID, MsgConnStatus, []byte{byte(StatusERR)}, nil)
		p.mu.Lock()
		delete(p.conns, st.connID)
		p.mu.Unlock()
		return
	}

	st.mu.Lock()
	st.targetConn = conn
	st.connected = true
	// 获取并清空缓存数据
	pending := st.pendingData
	st.pendingData = nil
	st.mu.Unlock()

	log.Printf("[服务端] %s 连接目标成功 %s, ID:%s", st.clientAddr, st.target, shortID(st.connID))

	// 发送连接成功（广播）
	_ = p.sendDownlink(st.connID, MsgConnStatus, []byte{byte(StatusOK)}, nil)

	// 发送缓存的数据
	if len(pending) > 0 {
		for _, data := range pending {
			if _, err := conn.Write(data); err != nil {
				log.Printf("[服务端] 写入缓存数据失败 %s: %v", st.target, err)
				p.unregisterConn(st.connID)
				return
			}
		}
	}

	// 启动目标→客户端转发
	go p.forwardTargetToClient(st)
}

func (p *ServerPool) handleTCPData(chID int, connID string, payload []byte) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil {
		return
	}

	st.mu.RLock()
	targetConn := st.targetConn
	st.mu.RUnlock()

	if targetConn == nil {
		// 连接还未建立，缓存数据
		st.mu.Lock()
		// 避免重复缓存：如果已经有缓存数据，就不再添加（广播消息可能重复）
		if st.targetConn == nil && len(st.pendingData) == 0 {
			st.pendingData = append(st.pendingData, payload)
		}
		st.mu.Unlock()
		return
	}

	// 只接受来自上行通道的数据，其余通道丢弃
	st.mu.RLock()
	uplinkChID := st.uplinkChID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		// 调试日志：需要时可解除注释
		// log.Printf("[服务端] 警告: 收到来自通道 %d 的数据，但上行通道是 %d, ID:%s，忽略",
		// 	chID, uplinkChID, shortID(connID))
		return
	}

	_, err := targetConn.Write(payload)
	if err != nil {
		log.Printf("[服务端] 写入目标失败 %s: %v", st.target, err)
		p.unregisterConn(connID)
	}
}

func (p *ServerPool) handleSelectDownlink(chID int, connID string, meta []byte) {
	// meta 包含客户端选择的下行通道号（4字节，大端序）
	var downlinkChID int
	if len(meta) >= 4 {
		downlinkChID = int(binary.BigEndian.Uint32(meta[0:4]))
	} else {
		// 兼容旧版本：使用当前发送消息的通道
		downlinkChID = chID
	}

	p.mu.RLock()
	st := p.conns[connID]
	wsConn := p.chConns[downlinkChID]
	p.mu.RUnlock()

	if st == nil {
		return
	}

	// 验证下行通道是否仍然活跃
	if wsConn == nil || wsConn.closed {
		log.Printf("[服务端] 警告: 客户端尝试选择已关闭的通道 %d 作为下行通道, connID:%s", downlinkChID, shortID(connID))
		return
	}

	// 验证消息是否从上行通道发送
	st.mu.RLock()
	uplinkChID := st.uplinkChID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		log.Printf("[服务端] 警告: MsgSelectDownlink 来自通道 %d，但上行通道是 %d, ID:%s，忽略",
			chID, uplinkChID, shortID(connID))
		return
	}

	st.mu.Lock()
	if st.downlinkChID == 0 {
		st.downlinkChID = downlinkChID
	} else {
		// 已经选择过下行通道，但收到另一个选择请求
		log.Printf("[服务端] 警告: %s 访问: %s, 当前下行通道 %d, 试图改为 %d, ID:%s，忽略",
			st.clientAddr, st.target, st.downlinkChID, downlinkChID, shortID(connID))
	}
	st.mu.Unlock()
}

func (p *ServerPool) handleTCPClose(chID int, connID string) {
	p.unregisterConn(connID)
}

func (p *ServerPool) forwardTargetToClient(st *ServerConnState) {
	defer p.unregisterConn(st.connID)

	buf := make([]byte, 32*1024)
	for {
		n, err := st.targetConn.Read(buf)
		if err != nil {
			// 检查是否是连接被其他 goroutine 关闭（正常情况）
			if err != io.EOF && !isNormalCloseError(err) {
				log.Printf("[服务端] 读取目标错误 %s: %v", st.target, err)
			}
			return
		}

		if n > 0 {
			_ = p.sendDownlink(st.connID, MsgTCPData, nil, buf[:n])
		}
	}
}

func (p *ServerPool) unregisterConn(connID string) {
	p.mu.Lock()
	st := p.conns[connID]
	delete(p.conns, connID)
	p.mu.Unlock()

	if st == nil {
		return
	}

	st.mu.Lock()
	// 防止重复注销
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true

	if !st.connected {
		st.mu.Unlock()
		return
	}
	st.connected = false

	target := st.target
	up := st.uplinkChID
	down := st.downlinkChID
	clientAddr := st.clientAddr

	if st.targetConn != nil {
		st.targetConn.Close()
	}
	if st.targetUDP != nil {
		st.targetUDP.Close()
	}
	st.mu.Unlock()

	u := "-"
	d := "-"
	if up > 0 {
		u = fmt.Sprintf("%d", up)
	}
	if down > 0 {
		d = fmt.Sprintf("%d", down)
	}

	log.Printf("[服务端] %s 访问: %s, 通道: TX %s RX %s, ID:%s, 已关闭",
		clientAddr, target, u, d, shortID(connID))
}

// ======================== UDP 处理 ========================

func (p *ServerPool) handleUDPConnect(chID int, connID string, meta []byte) {
	if len(meta) < 1 {
		p.sendDownlink(connID, MsgConnStatus, []byte{byte(StatusERR)}, nil)
		return
	}

	ipStrategy := meta[0]
	target := string(meta[1:])

	// 创建 UDP socket
	udpConn, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Printf("[服务端] 创建 UDP 失败: %v", err)
		p.sendDownlink(connID, MsgConnStatus, []byte{byte(StatusERR)}, nil)
		return
	}

	// 创建连接状态
	st := &ServerConnState{
		connID:     connID,
		target:     target,
		targetUDP:  udpConn,
		uplinkChID: chID,
		ipStrategy: ipStrategy,
		isUDP:      true,
		connected:  true,
	}

	p.mu.Lock()
	p.conns[connID] = st
	p.mu.Unlock()

	p.mu.RLock()
	wsConn := p.chConns[chID]
	p.mu.RUnlock()
	if wsConn != nil {
		st.clientID = wsConn.clientID
		st.clientAddr = wsConn.clientID
	}

	// 发送 MsgSelectUplink（广播），携带上行通道ID
	uplinkChIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
	_ = p.sendDownlink(connID, MsgSelectUplink, uplinkChIDBytes, nil)

	log.Printf("[服务端] %s UDP 访问: %s, 通道: TX %d, ID:%s", st.clientAddr, target, chID, shortID(connID))

	// 启动 UDP 接收
	go p.forwardUDPToClient(st)
}

func (p *ServerPool) handleUDPData(chID int, connID string, meta, payload []byte) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil || st.targetUDP == nil {
		return
	}

	// 只接受来自上行通道的数据，其余通道丢弃
	st.mu.RLock()
	uplinkChID := st.uplinkChID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		// 调试日志：需要时可解除注释
		// log.Printf("[服务端] 警告: 收到来自通道 %d 的 UDP 数据，但上行通道是 %d, ID:%s，忽略",
		// 	chID, uplinkChID, shortID(connID))
		return
	}

	// meta 是目标地址字符串
	targetAddr := string(meta)
	if targetAddr == "" {
		targetAddr = st.target
	}

	// IP 策略解析
	resolvedTarget := resolveWithStrategy(targetAddr, st.ipStrategy)

	// 解析 UDP 地址
	udpAddr, err := net.ResolveUDPAddr("udp", resolvedTarget)
	if err != nil {
		log.Printf("[服务端] 解析 UDP 地址失败 %s: %v", targetAddr, err)
		return
	}

	// 发送 UDP 包
	_, err = st.targetUDP.WriteToUDP(payload, udpAddr)
	if err != nil {
		log.Printf("[服务端] 发送 UDP 失败: %v", err)
	}
}

func (p *ServerPool) handleUDPClose(chID int, connID string) {
	p.unregisterConn(connID)
}

func (p *ServerPool) forwardUDPToClient(st *ServerConnState) {
	buf := make([]byte, 64*1024)
	natMap := make(map[string]string) // remoteAddr -> connID for responses

	defer p.unregisterConn(st.connID)

	for {
		st.mu.RLock()
		udpConn := st.targetUDP
		connected := st.connected
		st.mu.RUnlock()

		if !connected || udpConn == nil {
			return
		}

		udpConn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, addr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		if n > 0 {
			// 构造返回地址
			replyAddr := addr.String()
			natMap[replyAddr] = st.connID

			_ = p.sendDownlink(st.connID, MsgUDPData, []byte(replyAddr), buf[:n])
		}
	}
}
