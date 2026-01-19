package server

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"x-tunnel/common"
)

// serverPool 服务端连接池
type serverPool struct {
	config *Config
	token  string
	mu     sync.RWMutex

	// 连接状态映射
	conns map[string]*ServerConnState

	// WebSocket 连接
	wsConns []*ServerWSConn
	chConns map[int]*ServerWSConn

	nextChID int
}

// newServerPool 创建新的服务端连接池
func newServerPool(token string, config *Config) *serverPool {
	return &serverPool{
		config:   config,
		token:    token,
		conns:    make(map[string]*ServerConnState),
		wsConns:  make([]*ServerWSConn, 0),
		chConns:  make(map[int]*ServerWSConn),
		nextChID: 1,
	}
}

// upgrader WebSocket 升级器
var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// handleWebSocket 处理 WebSocket 连接
func (p *serverPool) handleWebSocket(w http.ResponseWriter, r *http.Request) {
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
		// 如果客户端没有提供 ch_id,服务端自动分配
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

// handleMessage 处理消息
func (p *serverPool) handleMessage(chID int, msgType common.MessageType, connID string, meta, payload []byte) {
	switch msgType {
	case common.MsgTCPConnect:
		p.handleTCPConnect(chID, connID, meta)

	case common.MsgTCPData:
		p.handleTCPData(chID, connID, payload)

	case common.MsgSelectDownlink:
		p.handleSelectDownlink(chID, connID, meta)

	case common.MsgTCPClose:
		p.handleTCPClose(chID, connID)

	case common.MsgUDPConnect:
		p.handleUDPConnect(chID, connID, meta)

	case common.MsgUDPData:
		p.handleUDPData(chID, connID, meta, payload)

	case common.MsgUDPClose:
		p.handleUDPClose(chID, connID)
	}
}

// sendDownlink 发送下行数据
func (p *serverPool) sendDownlink(connID string, msgType common.MessageType, meta, payload []byte) error {
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
		// 已选择下行通道:单播
		return p.sendToChannel(downlink, websocket.BinaryMessage, common.EncodeMessage(msgType, connID, meta, payload))
	}

	// 未选择:广播
	return p.broadcastWrite(websocket.BinaryMessage, common.EncodeMessage(msgType, connID, meta, payload))
}

// broadcastWrite 广播写入所有通道
func (p *serverPool) broadcastWrite(msgType int, data []byte) error {
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

	// 发送到所有活跃通道,忽略写队列满错误
	for _, wsConn := range activeConns {
		_ = wsConn.asyncWrite(msgType, data)
	}
	return nil
}

// sendToChannel 发送到指定通道
func (p *serverPool) sendToChannel(chID int, msgType int, data []byte) error {
	p.mu.RLock()
	wsConn := p.chConns[chID]
	p.mu.RUnlock()

	if wsConn == nil || wsConn.closed {
		return fmt.Errorf("通道 %d 不可用", chID)
	}

	_ = wsConn.asyncWrite(msgType, data)
	return nil
}

// cleanupChannel 清理通道
func (p *serverPool) cleanupChannel(chID int) {
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

// unregisterConn 注销连接
func (p *serverPool) unregisterConn(connID string) {
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
		clientAddr, target, u, d, common.ShortID(connID))
}

// Stats 返回统计信息
func (p *serverPool) Stats() *ServerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	activeConns := 0
	for _, wsConn := range p.wsConns {
		if wsConn != nil && !wsConn.closed {
			activeConns++
		}
	}

	return &ServerStats{
		ActiveConnections: activeConns,
		ActiveChannels:    activeConns,
		TotalConnections:  int64(len(p.conns)),
	}
}
