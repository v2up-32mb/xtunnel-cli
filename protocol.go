package main

import (
	"encoding/binary"
	"errors"
)

// ======================== 二进制协议 ========================

type MessageType uint8

const (
	MsgTCPConnect MessageType = iota + 1
	MsgTCPData
	MsgTCPClose
	MsgUDPConnect
	MsgUDPData
	MsgUDPClose
	MsgConnStatus
	MsgUplink
	MsgSelectDownlink
)

type ConnStatus uint8

const (
	StatusOK  ConnStatus = 0
	StatusERR ConnStatus = 1
)

const headerLen = 8

func encodeMessage(t MessageType, connID string, meta, payload []byte) []byte {
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

func decodeMessage(b []byte) (t MessageType, connID string, meta, payload []byte, err error) {
	if len(b) < headerLen {
		return 0, "", nil, nil, errors.New("帧过短")
	}
	t = MessageType(b[0])
	idLen := int(b[1])
	metaLen := int(binary.BigEndian.Uint16(b[2:4]))
	payloadLen := int(binary.BigEndian.Uint32(b[4:8]))
	total := headerLen + idLen + metaLen + payloadLen
	if idLen < 0 || metaLen < 0 || payloadLen < 0 || total < headerLen || total > len(b) {
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

// ======================== IP 策略 ========================

const (
	IPStrategyDefault  byte = 0
	IPStrategyIPv4Only byte = 1
	IPStrategyIPv6Only byte = 2
	IPStrategyPv4Pv6   byte = 3
	IPStrategyPv6Pv4   byte = 4
)
