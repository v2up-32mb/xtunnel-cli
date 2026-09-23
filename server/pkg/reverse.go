package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

type ServerReverseConn struct {
	connID       string
	target       string
	ownerClientID string
	sendCh      int32 // P1, atomic
	recvCh      int32 // P2, atomic
	pipe        net.Conn
	result      chan struct{}
	ok          bool
	errMsg      string
	closed      int32 // atomic flag
}

func (p *serverPool) newServerReverseConn(connID, clientID, target string) *ServerReverseConn {
	return &ServerReverseConn{
		connID:        connID,
		target:        target,
		ownerClientID: clientID,
		result:        make(chan struct{}),
	}
}

type reverseDialer struct {
	pool     *serverPool
	clientID string
}

// DialStream implements dialer.Dialer for reverse mode.
func (d *reverseDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	// ensure client has at least one active channel
	if d.pool.countActiveChannelsForClient(d.clientID) == 0 {
		return nil, fmt.Errorf("无可用客户端通道")
	}
	connID := uuid.NewString()

	rc := d.pool.newServerReverseConn(connID, d.clientID, target)

	// register
	d.pool.revMu.Lock()
	d.pool.revConns[connID] = rc
	d.pool.revMu.Unlock()

	// broadcast MsgTCPConnect to client's channels
	meta := make([]byte, 1+len(target))
	meta[0] = byte(protocol.IPStrategyDefault)
	copy(meta[1:], []byte(target))
	_ = d.pool.broadcastWriteToClient(d.clientID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil))

	// wait for result
	select {
	case <-rc.result:
	case <-time.After(d.pool.connectTimeout()):
		d.pool.removeReverseConn(connID)
		return nil, fmt.Errorf("连接 %s 超时", target)
	case <-ctx.Done():
		d.pool.removeReverseConn(connID)
		return nil, ctx.Err()
	}

	if !rc.ok {
		return nil, fmt.Errorf("拨号失败: %s", rc.errMsg)
	}

	// create pipe
	serverSide, clientSide := net.Pipe()
	rc.pipe = clientSide

	// pump upstream from pipe to client
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := serverSide.Read(buf)
			if err != nil {
				// close remote
				sendCh := atomic.LoadInt32(&rc.sendCh)
				if sendCh > 0 {
					_ = d.pool.sendToChannel(d.clientID, int(sendCh), websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil))
				}
				d.pool.removeReverseConn(connID)
				return
			}
			if n > 0 {
				sendCh := atomic.LoadInt32(&rc.sendCh)
				if sendCh > 0 {
					_ = d.pool.sendToChannel(d.clientID, int(sendCh), websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, buf[:n]))
				} else {
					// not known yet: broadcast
					_ = d.pool.broadcastWriteToClient(d.clientID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, buf[:n]))
				}
			}
		}
	}()

	return clientSide, nil
}

func (p *serverPool) handleReverseMessage(clientID string, chID int, msgType protocol.MessageType, connID string, meta, payload []byte) {
	rc := p.getReverseConn(connID)
	if rc == nil || rc.ownerClientID != clientID {
		return
	}
	switch msgType {
	case protocol.MsgSelectUplink:
		// first arrival wins sendCh
		if atomic.LoadInt32(&rc.sendCh) == 0 && len(meta) >= 4 {
			send := int(binary.BigEndian.Uint32(meta[:4]))
			if atomic.CompareAndSwapInt32(&rc.sendCh, 0, int32(send)) {
				// set recvCh to this channel (first arrival)
				if atomic.CompareAndSwapInt32(&rc.recvCh, 0, int32(chID)) {
					// notify client of downlink choice
					downMeta := make([]byte, 4)
					binary.BigEndian.PutUint32(downMeta, uint32(chID))
					_ = p.sendToChannel(clientID, send, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, downMeta, nil))
				}
			}
		}
	case protocol.MsgConnStatus:
		if atomic.CompareAndSwapInt32(&rc.closed, 0, 1) { // use closed flag as guard
			if len(meta) > 0 && meta[0] == byte(protocol.StatusOK) {
				rc.ok = true
			} else {
				rc.ok = false
				if len(meta) > 0 {
					rc.errMsg = string(meta)
				} else {
					rc.errMsg = "服务端返回错误"
				}
			}
			close(rc.result)
		}
	case protocol.MsgTCPData:
		// only accept from recvCh
		if int(atomic.LoadInt32(&rc.recvCh)) != chID {
			return
		}
		if rc.pipe != nil {
			_ = rc.pipe.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
			_, err := rc.pipe.Write(payload)
			if err != nil {
				// close
				sendCh := atomic.LoadInt32(&rc.sendCh)
				if sendCh > 0 {
					_ = p.sendToChannel(clientID, int(sendCh), websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil))
				}
				p.removeReverseConn(connID)
			}
		}
	case protocol.MsgTCPClose:
		if int(atomic.LoadInt32(&rc.recvCh)) != chID {
			return
		}
		p.removeReverseConn(connID)
	}
}

func (p *serverPool) removeReverseConn(connID string) {
	p.revMu.Lock()
	rc, ok := p.revConns[connID]
	if ok {
		delete(p.revConns, connID)
	}
	p.revMu.Unlock()
	if ok && rc.pipe != nil {
		_ = rc.pipe.Close()
	}
}

func (p *serverPool) getReverseConn(connID string) *ServerReverseConn {
	p.revMu.RLock()
	rc := p.revConns[connID]
	p.revMu.RUnlock()
	return rc
}
