package server

import (
	"MQ/abstract"
	"MQ/log"
	"net"
	"sync"
)

const (
	typeConsumer = iota
	typeProducer
)

type Client interface {
	Type() int
	stats(string) int
}

type tcpServer struct {
	nsqd  *NSQD
	conns sync.Map //所有的连接
}

func (p *tcpServer) Handle(conn net.Conn) {
	var prot abstract.Protocol
	client := prot.NewClient(conn)           //创建型的client(IOLOOP)
	p.conns.Store(conn.RemoteAddr(), client) //存入
	err := prot.IOLOOP(client)
	if err != nil {
		log.Error("client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	client.Close()

}
