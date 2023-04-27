package abstract

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close() error
}

type Protocol interface {
	NewClient(net.Conn) Client
	IOLOOP(Client) error
}

// 写函数
// 加入长度前缀
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// 写函数
// 加入长度前缀和帧头
func SendFramedResponse(w io.Writer, Type int32, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)+4))
	if err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.BigEndian, (uint32)(Type))
	if err != nil {
		return 4, err
	}
	n, err := w.Write(data)
	if err != nil {
		return n + 8, err
	}

	return n + 8, err
}
