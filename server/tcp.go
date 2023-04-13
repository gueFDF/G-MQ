package server

import (
	"context"
	"myMQ/logs"
	"net"
	"sync"
)

// 用来管理所有的tcp连接
type TcpServer1 struct {
	ctx   *context.Context
	conns sync.Map
}

// 初步构想，将管理权移至nsqd
func (p *TcpServer1) Handle(conn net.Conn) {
	logs.Info("TCP:new client(%s)", conn.RemoteAddr())
	client := NewClient(conn, conn.RemoteAddr().String())
	p.conns.Store(conn.RemoteAddr(), client)
	client.Handle(*p.ctx)

}

func (p *TcpServer1) Close() {
	p.conns.Range(func(key, value any) bool {
		c := value.(Client)
		c.Close()
		return true
	})
}

func (p *TcpServer1) Start() {
	
}
func TcpServer(ctx context.Context, addr, port string) {
	fqAddress := addr + ":" + port
	listener, err := net.Listen("tcp", fqAddress)
	if err != nil {
		panic("tcp listen(" + fqAddress + ") failed")
	}
	logs.Info("listening for clients on %s", fqAddress)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				panic("accept failed: " + err.Error())
			}
			client := NewClient(conn, conn.RemoteAddr().String())
			go client.Handle(ctx)
		}
	}
}
