package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// ServerReverseConn 服务端反向连接状态（请求方）。
// sendCh = P1（客户端选定的服务端发包通道，来自 MsgSelectUplink meta）；
// recvCh = P2（服务端竞争获胜的收包通道，即 MsgSelectUplink 的首达通道）。
type ServerReverseConn struct {
	connID        string
	target        string
	ownerClientID string
	sendCh        int32 // atomic
	recvCh        int32 // atomic

	pipe net.Conn // 隧道侧端：MsgTCPData 写入此端（缓冲管道，慢读不阻塞读循环）
	app  net.Conn // 应用侧端：DialStream 成功后返回给 xshared 隧道泵

	settled int32 // atomic：result 是否已定型（防止 close 后再次 close panic）
	result  chan struct{}
	ok      bool
	errMsg  string
}

func newServerReverseConn(connID, clientID, target string) *ServerReverseConn {
	appEnd, tunEnd := xtunnel.NewBufferedPipe()
	return &ServerReverseConn{
		connID:        connID,
		target:        target,
		ownerClientID: clientID,
		pipe:          tunEnd,
		app:           appEnd,
		result:        make(chan struct{}),
	}
}

// settle 定型结果（幂等）：首个调用方生效并唤醒等待者。
func (rc *ServerReverseConn) settle(ok bool, msg string) {
	if atomic.CompareAndSwapInt32(&rc.settled, 0, 1) {
		rc.ok = ok
		rc.errMsg = msg
		close(rc.result)
	}
}

func (rc *ServerReverseConn) isSettled() bool {
	return atomic.LoadInt32(&rc.settled) == 1
}

type reverseDialer struct {
	pool     *serverPool
	clientID string
}

// DialStream 实现 dialer.Dialer（反向模式请求方）：
// 广播 MsgTCPConnect → 竞争 MsgSelectUplink 定 P1/P2 → 经 P1 回 MsgSelectDownlink
// → 等 MsgConnStatus → 返回应用侧管道端。
func (d *reverseDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	if d.pool.countActiveChannelsForClient(d.clientID) == 0 {
		return nil, fmt.Errorf("无可用客户端通道")
	}
	connID := uuid.NewString()
	rc := newServerReverseConn(connID, d.clientID, target)

	d.pool.revMu.Lock()
	d.pool.revConns[connID] = rc
	d.pool.revMu.Unlock()

	meta := make([]byte, 1+len(target))
	meta[0] = byte(protocol.IPStrategyDefault)
	copy(meta[1:], []byte(target))
	msg := protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)

	// Hot Pair 路径：预热 Pair 就绪则单播直达，免广播竞争（镜像正向 RegisterAndBroadcastTCP）
	var pair *ReverseHotPair
	if d.pool.reversePairWarmer != nil {
		pair = d.pool.reversePairWarmer.AcquirePair(d.clientID)
	}
	if pair != nil {
		atomic.StoreInt32(&rc.sendCh, int32(pair.P1))
		atomic.StoreInt32(&rc.recvCh, int32(pair.P2))
		if err := d.pool.sendToChannel(d.clientID, pair.P1, websocket.BinaryMessage, msg); err != nil {
			// Pair 通道已失效：废弃该通道全部 Pair 并回退广播竞争
			log.Printf("[服务端] Hot Pair 通道 %d 发送失败，回退广播: %v", pair.P1, err)
			d.pool.reversePairWarmer.InvalidateChannel(pair.P1)
			pair = nil
		} else {
			downMeta := make([]byte, 4)
			binary.BigEndian.PutUint32(downMeta, uint32(pair.P2))
			_ = d.pool.sendToChannel(d.clientID, pair.P1, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, downMeta, nil))
		}
	}
	if pair == nil {
		if err := d.pool.broadcastWriteToClient(d.clientID, websocket.BinaryMessage, msg); err != nil {
			d.pool.removeReverseConn(connID)
			return nil, fmt.Errorf("无可用客户端通道")
		}
	}

	// 上行泵：应用侧 → 隧道（P1 已知则单播，否则广播）
	go d.pool.reverseUpstreamPump(rc)

	// 等待拨号结果
	timer := time.NewTimer(d.pool.connectTimeout())
	defer timer.Stop()
	select {
	case <-rc.result:
	case <-timer.C:
		rc.settle(false, "拨号超时")
		// 尽力通知客户端清理，避免客户端侧连接泄漏
		d.pool.reverseNotifyClose(rc)
		d.pool.removeReverseConn(connID)
		return nil, fmt.Errorf("连接 %s 超时", target)
	case <-ctx.Done():
		rc.settle(false, ctx.Err().Error())
		d.pool.reverseNotifyClose(rc)
		d.pool.removeReverseConn(connID)
		return nil, ctx.Err()
	}

	if !rc.ok {
		d.pool.removeReverseConn(connID)
		return nil, fmt.Errorf("拨号失败: %s", rc.errMsg)
	}

	return rc.app, nil
}

// reverseUpstreamPump 应用侧数据 → 隧道。
func (p *serverPool) reverseUpstreamPump(rc *ServerReverseConn) {
	connID := rc.connID
	buf := make([]byte, 64*1024)
	for {
		n, err := rc.pipe.Read(buf)
		if err != nil {
			// 应用侧关闭（SOCKS5 隧道拆除属正常路径）：通知客户端关闭并清理
			p.reverseNotifyClose(rc)
			p.removeReverseConn(connID)
			return
		}
		if n > 0 {
			sendCh := int(atomic.LoadInt32(&rc.sendCh))
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendCh > 0 {
				_ = p.sendToChannel(rc.ownerClientID, sendCh, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
			} else {
				_ = p.broadcastWriteToClient(rc.ownerClientID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
			}
		}
	}
}

// reverseNotifyClose 尽力通知客户端关闭反向连接（P1 已知则单播，否则广播）。
func (p *serverPool) reverseNotifyClose(rc *ServerReverseConn) {
	msg := protocol.EncodeMessage(protocol.MsgTCPClose, rc.connID, nil, nil)
	sendCh := int(atomic.LoadInt32(&rc.sendCh))
	if sendCh > 0 {
		_ = p.sendToChannel(rc.ownerClientID, sendCh, websocket.BinaryMessage, msg)
	} else {
		_ = p.broadcastWriteToClient(rc.ownerClientID, websocket.BinaryMessage, msg)
	}
}

// handleReverseMessage 处理来自客户端的反向连接消息（handleMessage 前置拦截后进入）。
func (p *serverPool) handleReverseMessage(clientID string, chID int, msgType protocol.MessageType, connID string, meta, payload []byte) {
	rc := p.getReverseConn(connID)
	if rc == nil {
		return
	}
	switch msgType {
	case protocol.MsgSelectUplink:
		// 首达竞争：sendCh 来自 meta（P1），recvCh 取首达通道（P2）
		if len(meta) < 4 {
			return
		}
		if atomic.LoadInt32(&rc.sendCh) != 0 {
			return // 已有通道获胜
		}
		send := int32(binary.BigEndian.Uint32(meta[:4]))
		if atomic.CompareAndSwapInt32(&rc.sendCh, 0, send) {
			if atomic.CompareAndSwapInt32(&rc.recvCh, 0, int32(chID)) {
				downMeta := make([]byte, 4)
				binary.BigEndian.PutUint32(downMeta, uint32(chID))
				_ = p.sendToChannel(clientID, int(send), websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, downMeta, nil))
			}
		}

	case protocol.MsgConnStatus:
		if len(meta) < 1 {
			return
		}
		if protocol.ConnStatus(meta[0]) == protocol.StatusOK {
			rc.settle(true, "")
		} else {
			reason := ""
			if len(meta) > 1 {
				reason = string(meta[1:])
			}
			rc.settle(false, reason)
		}

	case protocol.MsgTCPData:
		// 只接受来自 P2（服务端选定收包通道）的数据
		if int(atomic.LoadInt32(&rc.recvCh)) != chID {
			return
		}
		_ = rc.pipe.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
		_, err := rc.pipe.Write(payload)
		_ = rc.pipe.SetWriteDeadline(time.Time{})
		if err != nil {
			p.reverseNotifyClose(rc)
			p.removeReverseConn(connID)
		}

	case protocol.MsgTCPClose:
		p.reverseNotifyClose(rc)
		p.removeReverseConn(connID)
	}
}

// removeReverseConn 移除反向连接：关管道、唤醒等待者。
func (p *serverPool) removeReverseConn(connID string) {
	p.revMu.Lock()
	rc, ok := p.revConns[connID]
	if ok {
		delete(p.revConns, connID)
	}
	p.revMu.Unlock()
	if !ok {
		return
	}
	rc.settle(false, "connection closed")
	_ = rc.pipe.Close()
	_ = rc.app.Close()
}

func (p *serverPool) getReverseConn(connID string) *ServerReverseConn {
	p.revMu.RLock()
	rc := p.revConns[connID]
	p.revMu.RUnlock()
	return rc
}
