package server

import (
	"MQ/abstract"
	"MQ/mlog"
	"MQ/util"
	"net"
	"sync"
)

type NSQD struct {
	clientID      int64
	tcpServer     *tcpServer //管理所有connect
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	rwmutex       sync.RWMutex

	waitgroup util.WaitGroupWrapper //管理子协程
	dl        *util.DirLock         //封装的文件锁
}

func (n *NSQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				mlog.Fatal("%s", err)
			}
			exitCh <- err
		})
	}

	//启动TCPserver
	n.waitgroup.Wrap(func() {
		exitFunc(abstract.TCPServer(n.tcpListener, n.tcpServer))
	})
	//启动http
	if n.httpListener != nil {
		httpServer := newHTTPServer(n, false, false)
		n.waitgroup.Wrap(func() {
			exitFunc(abstract.Serve(n.httpListener, httpServer, "HTTP"))
		})
	}
	//启动https
	if n.httpsListener != nil {
		httpServer := newHTTPServer(n, false, false)
		n.waitgroup.Wrap(func() {
			exitFunc(abstract.Serve(n.httpsListener, httpServer, "HTTPS"))
		})
	}
	//TODO 启动queueScanLoop 数据处理协程

	//TODO 启动服务发现

	err := <-exitCh
	return err
}
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}
