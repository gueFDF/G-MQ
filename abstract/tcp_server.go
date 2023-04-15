package abstract

import (
	"MQ/log"
	"net"
	"sync"
)

// tcpserver抽象层
type TCPHandler interface {
	Handle(net.Conn)
}

// 启动监听，并且handler
func TCPServer(listener net.Listener, handler TCPHandler) error {
	log.Info("TCP: listening on %s", listener.Addr())
	var wg sync.WaitGroup
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}
}
