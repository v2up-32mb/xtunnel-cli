// Package common 提供 client 和 server 共享的二进制协议定义
package common

import (
	"encoding/binary"
	"errors"
)

// ======================== 二进制协议 ========================

// MessageType 消息类型
type MessageType uint8

const (
	MsgTCPConnect MessageType = iota + 1
	MsgTCPData
	MsgTCPClose
	MsgUDPConnect
	MsgUDPData
	MsgUDPClose
	MsgConnStatus
	MsgSelectUplink
	MsgSelectDownlink
)

// ConnStatus 连接状态
type ConnStatus uint8

const (
	StatusOK  ConnStatus = 0
	StatusERR ConnStatus = 1
)

const headerLen = 8

// EncodeMessage 编码消息
func EncodeMessage(t MessageType, connID string, meta, payload []byte) []byte {
	if len(connID) > 255 {
		connID = connID[:255]
	}
	buf := make([]byte, headerLen+len(connID)+len(meta)+len(payload))
	buf[0] = byte(t)
	buf[1] = byte(len(connID))
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(meta)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	off := headerLen
	copy(buf[off:], connID)
	off += len(connID)
	copy(buf[off:], meta)
	off += len(meta)
	copy(buf[off:], payload)
	return buf
}

// DecodeMessage 解码消息
func DecodeMessage(b []byte) (t MessageType, connID string, meta, payload []byte, err error) {
	if len(b) < headerLen {
		return 0, "", nil, nil, errors.New("帧过短")
	}
	t = MessageType(b[0])
	idLen := int(b[1])
	metaLen := int(binary.BigEndian.Uint16(b[2:4]))
	payloadLen := int(binary.BigEndian.Uint32(b[4:8]))
	total := headerLen + idLen + metaLen + payloadLen
	// 注意: idLen 来自 uint8 转 int, 不会为负; metaLen 和 payloadLen 来自固定宽度无符号整数转 int
	// 在 32 位系统上, payloadLen 可能溢出, 检查 total 是否小于 headerLen
	if total < headerLen || total > len(b) {
		return 0, "", nil, nil, errors.New("长度无效")
	}
	off := headerLen
	connID = string(b[off : off+idLen])
	off += idLen
	meta = b[off : off+metaLen]
	off += metaLen
	payload = b[off : off+payloadLen]
	return t, connID, meta, payload, nil
}
