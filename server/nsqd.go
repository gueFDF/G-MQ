package server

import (
	"net"
	"sync"
)

type NSQD struct {
	clientID      int64
	tcpServer     *tcpServer //管理所有connect
	httpsListener net.Listener
	rwmutex       sync.RWMutex
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}
