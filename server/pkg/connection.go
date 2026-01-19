package server

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"x-tunnel/common"
)

// writeTask 写入任务
type writeTask struct {
	msgType int
	data    []byte
}

// ServerConnState 服务端连接状态
type ServerConnState struct {
	connID       string
	clientID     string
	target       string
	targetConn   net.Conn
	targetUDP    *net.UDPConn
	uplinkChID   int
	downlinkChID int
	ipStrategy   common.IPStrategy
	isUDP        bool
	clientAddr   string
	connected    bool
	closed       bool // 防止重复注销
	mu           sync.RWMutex
	pendingData  [][]byte // 连接建立前到达的数据缓存
}

// ServerWSConn WebSocket 连接
type ServerWSConn struct {
	ws        *websocket.Conn
	chID      int
	clientID  string
	pool      *serverPool
	mu        sync.Mutex
	closed    bool
	writeChan chan writeTask
}

// start 启动写入协程
func (wsConn *ServerWSConn) start() {
	go wsConn.writeLoop()
}

// readLoop 读取循环
func (wsConn *ServerWSConn) readLoop() {
	defer func() {
		if !wsConn.closed {
			wsConn.close()
		}
	}()

	wsConn.ws.SetPongHandler(func(string) error {
		return wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
	})
	wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
	wsConn.ws.SetPingHandler(func(m string) error {
		wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
		_ = wsConn.asyncWrite(websocket.PongMessage, []byte(m))
		// pong 发送失败不影响 ping/pong 循环，总是返回 nil
		return nil
	})

	for {
		mt, msg, err := wsConn.ws.ReadMessage()
		if err != nil {
			if !common.IsNormalCloseError(err) {
				log.Printf("[服务端] 通道 %d 读取消息失败: %v", wsConn.chID, err)
			} else {
				log.Printf("[服务端] 通道 %d 正常关闭: %v", wsConn.chID, err)
			}
			return
		}
		// 每次成功读取消息后重置读超时
		wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))

		if mt != websocket.BinaryMessage {
			continue
		}

		msgType, connID, meta, payload, err := common.DecodeMessage(msg)
		if err != nil {
			continue
		}

		wsConn.pool.handleMessage(wsConn.chID, msgType, connID, meta, payload)
	}
}

// writeLoop 写入循环
func (wsConn *ServerWSConn) writeLoop() {
	ticker := time.NewTicker(wsConn.pool.config.PingInterval)
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
				wsConn.close()
				return
			}
		}
	}
}

// asyncWrite 异步写入
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

// writeDirect 直接写入
func (wsConn *ServerWSConn) writeDirect(msgType int, data []byte) error {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()

	if wsConn.closed {
		return nil
	}

	if msgType != websocket.BinaryMessage {
		_ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
		if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
			return err
		}
		_ = wsConn.ws.SetWriteDeadline(time.Time{})
		return nil
	}

	_ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
	if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
		return err
	}
	_ = wsConn.ws.SetWriteDeadline(time.Time{})
	return nil
}

// close 关闭连接
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
