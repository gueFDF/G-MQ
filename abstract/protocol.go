package abstract

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close()
}

type Protocol interface {
	NewClient(net.Conn) Client
	IOLOOP(Client) error
}

// 写消息，处理黏包
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
