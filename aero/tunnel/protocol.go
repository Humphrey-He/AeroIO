package tunnel

import "encoding/binary"

// 协议魔数
const Magic = 0xA1B2

// 消息类型
const (
	MsgRegister   = 0x01 // Agent 注册
	MsgOpenPort  = 0x02 // 请求开放端口
	MsgClosePort = 0x03 // 关闭端口
	MsgData      = 0x04  // 数据转发
	MsgHeartbeat = 0x05  // 心跳
	MsgAck       = 0x06  // 确认响应
	MsgError     = 0x07  // 错误响应
	MsgListPorts = 0x08  // 查询可用端口
)

// HeaderSize 控制消息头大小
const HeaderSize = 15 // 2(Magic) + 1(Type) + 8(ChannelID) + 4(Length)

// ChannelID 特殊值
const (
	ChannelIDControl = 0 // 控制通道 ID
)

// Message 是隧道协议的消息结构
type Message struct {
	Type      byte
	ChannelID uint64
	Payload   []byte
}

// NewMessage 创建新消息
func NewMessage(msgType byte, channelID uint64, payload []byte) *Message {
	return &Message{
		Type:      msgType,
		ChannelID: channelID,
		Payload:   payload,
	}
}

// NewRegisterMessage 创建注册消息
func NewRegisterMessage(agentID string) *Message {
	return &Message{
		Type:      MsgRegister,
		ChannelID: ChannelIDControl,
		Payload:   []byte(agentID),
	}
}

// NewOpenPortMessage 创建开放端口请求消息
func NewOpenPortMessage(port uint16, target string) *Message {
	payload := make([]byte, 4+len(target))
	binary.BigEndian.PutUint16(payload[0:2], port)
	binary.BigEndian.PutUint16(payload[2:4], uint16(len(target)))
	copy(payload[4:], target)
	return &Message{
		Type:      MsgOpenPort,
		ChannelID: ChannelIDControl,
		Payload:   payload,
	}
}

// NewClosePortMessage 创建关闭端口消息
func NewClosePortMessage(channelID uint64) *Message {
	return &Message{
		Type:      MsgClosePort,
		ChannelID: channelID,
		Payload:   nil,
	}
}

// NewDataMessage 创建数据消息
func NewDataMessage(channelID uint64, data []byte) *Message {
	return &Message{
		Type:      MsgData,
		ChannelID: channelID,
		Payload:   data,
	}
}

// NewHeartbeatMessage 创建心跳消息
func NewHeartbeatMessage() *Message {
	return &Message{
		Type:      MsgHeartbeat,
		ChannelID: ChannelIDControl,
		Payload:   nil,
	}
}

// NewAckMessage 创建确认消息
func NewAckMessage(channelID uint64) *Message {
	return &Message{
		Type:      MsgAck,
		ChannelID: channelID,
		Payload:   nil,
	}
}

// NewErrorMessage 创建错误消息
func NewErrorMessage(channelID uint64, errMsg string) *Message {
	return &Message{
		Type:      MsgError,
		ChannelID: channelID,
		Payload:   []byte(errMsg),
	}
}

// PortInfo 端口映射信息
type PortInfo struct {
	PublicPort uint16
	TargetAddr string
	ChannelID  uint64
	Status     string
}
