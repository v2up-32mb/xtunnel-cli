//go:build client

package main

import (
	"encoding/binary"
	"errors"
)

// ======================== 二进制协议 ========================
//
// 消息格式（二进制帧）：
// +-----+------+----------+----------+--------+---------+
// | Type| IDLen| MetaLen  | PayLen   | ConnID | Meta    |
// |(1B) | (1B) |  (2B)    |  (4B)    |(var)   | (var)   |
// +-----+------+----------+----------+--------+---------+
// | Payload (var)                                            |
// +----------------------------------------------------------+
//
// Type: 消息类型（MessageType）
// IDLen: 连接 ID 长度
// MetaLen: 元数据长度
// PayLen: 负载长度
// ConnID: 连接唯一标识符
// Meta: 元数据（如目标地址、IP 策略等）
// Payload: 实际数据

// MessageType 消息类型枚举
type MessageType uint8

const (
	// MsgTCPConnect TCP 连接请求消息
	// Meta: [IPStrategy(1B)][TargetAddress]
	// Payload: 可选的首批数据
	MsgTCPConnect MessageType = iota + 1

	// MsgTCPData TCP 数据传输消息
	// Meta: 保留（当前为空）
	// Payload: 实际 TCP 数据
	MsgTCPData

	// MsgTCPClose TCP 连接关闭消息
	// Meta: 保留
	// Payload: 空
	MsgTCPClose

	// MsgUDPConnect UDP 连接请求消息
	// Meta: [IPStrategy(1B)][TargetAddress]
	// Payload: 空
	MsgUDPConnect

	// MsgUDPData UDP 数据传输消息
	// Meta: 目标地址字符串（用于响应路由）
	// Payload: 实际 UDP 数据
	MsgUDPData

	// MsgUDPClose UDP 连接关闭消息
	// Meta: 保留
	// Payload: 空
	MsgUDPClose

	// MsgConnStatus 连接状态通知消息
	// Meta: [Status(1B)]
	// Payload: 空
	MsgConnStatus

	// MsgSelectUplink 服务端选择上行通道消息
	// Meta: [ChannelID(4B)]（大端序）
	// Payload: 空
	MsgSelectUplink

	// MsgSelectDownlink 客户端选择下行通道消息
	// Meta: [ChannelID(4B)]（大端序）
	// Payload: 空
	MsgSelectDownlink
)

// ConnStatus 连接状态枚举
type ConnStatus uint8

const (
	// StatusOK 连接成功
	StatusOK ConnStatus = 0
	// StatusERR 连接失败
	StatusERR ConnStatus = 1
)

const headerLen = 8

// encodeMessage 编码消息为二进制格式
//
// 参数:
//   - t: 消息类型
//   - connID: 连接唯一标识符（最大 255 字节）
//   - meta: 元数据
//   - payload: 负载数据
//
// 返回编码后的字节切片
func encodeMessage(t MessageType, connID string, meta, payload []byte) []byte {
	// 限制连接 ID 长度
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

// decodeMessage 解码二进制消息
//
// 参数:
//   - b: 二进制数据
//
// 返回消息类型、连接 ID、元数据、负载和可能的错误
func decodeMessage(b []byte) (t MessageType, connID string, meta, payload []byte, err error) {
	if len(b) < headerLen {
		return 0, "", nil, nil, errors.New("帧过短")
	}
	t = MessageType(b[0])
	idLen := int(b[1])
	metaLen := int(binary.BigEndian.Uint16(b[2:4]))
	payloadLen := int(binary.BigEndian.Uint32(b[4:8]))
	total := headerLen + idLen + metaLen + payloadLen
	// 验证长度有效性（防止溢出攻击）
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
//
// IP 策略用于控制 DNS 解析时优先使用哪种 IP 地址类型（IPv4/IPv6）

const (
	// IPStrategyDefault 使用系统默认的 DNS 解析顺序
	IPStrategyDefault byte = 0
	// IPStrategyIPv4Only 仅使用 IPv4 地址
	IPStrategyIPv4Only byte = 1
	// IPStrategyIPv6Only 仅使用 IPv6 地址
	IPStrategyIPv6Only byte = 2
	// IPStrategyPv4Pv6 优先使用 IPv4，失败时尝试 IPv6
	IPStrategyPv4Pv6 byte = 3
	// IPStrategyPv6Pv4 优先使用 IPv6，失败时尝试 IPv4
	IPStrategyPv6Pv4 byte = 4
)
