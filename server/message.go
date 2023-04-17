package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// 此文件用来封装mq的message协议
const (
	MsgIDLength       = 16                  //ID长度
	minValidMsgLength = MsgIDLength + 8 + 2 // ID长度(16 bytes)+Timestamp（时间戳，8 byte） + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte //消息内容
	Timestamp int64  //时间戳,创建时间
	Attempts  uint16 //尝试重发次数

	//flighting handle
	dis_time int64         //消息分发时间time.Now()
	clientID int64         //客户端ID
	pri      int64         //优先级，也就是该消息的过期时间，快过期的消息优先级高(time.Now().Add(timeout).UnixNano())
	index    int           //用于优先级队列，定位索引
	deferred time.Duration //延时发布时间，用于实现延时消息类型
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

//消息协议
/*
timestap(8-byte)+Attempts(2-byte)+message ID(16-byte) + message body(n-byte)
*/

// 将 Message 内部的 Timestamp、Attempts、ID 和 Body 输出到指定IO端
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var total int64
	var buf [10]byte //减少write调用次数
	//binary.Write 底层其实是调用 binary.*Endian.Put*+write ,所以没有直接使用高效
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))  //时间戳
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts)) //尝试次数
	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:]) //写入ID
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body) //写入Body
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// 解析消息
func decodeMessage(b []byte) (*Message, error) {
	var msg Message
	if len(b) < minValidMsgLength { //消息长度无效，小于最小长度
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8])) //获取时间戳
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])       //获取尝试次数
	copy(msg.ID[:], b[10:10+MsgIDLength])                 //获取消息ID
	msg.Body = b[10+MsgIDLength:]                         //获取消息内容

	return &msg, nil
}

//TODO:  writeMessageToBackend 用于写入磁盘队列
