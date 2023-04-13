package nsqd

import (
	"context"
	"myMQ/logs"
	"myMQ/server"
	"net"
	"sync"
)

// 对资源进行管理
type nsqd struct {
	tcpListener  net.Listener
	httpListener net.Listener
	tcpserver    *server.TcpServer1
	ctx          context.Context
}

func New() *nsqd {
	v := &nsqd{}
	return v
}

func Main() error {
	exitCh := make(chan error)
	var once sync.Once
	//保证只执行一次
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				logs.Fatal(err)
			}
			exitCh <- err
		})
	}

	exitFunc()
}
