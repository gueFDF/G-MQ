package server

import (
	"MQ/abstract"
	"MQ/mlog"
	"MQ/util"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// 类型
const (
	frameTypeResponse int32 = iota //response
	frameTypeError                 //error
	frameTypeMessage               //message
)

var separatorBytes = []byte(" ")           //用来分隔
var heartbeatBytes = []byte("_heartbeat_") //心跳
var okBytes = []byte("OK")                 //ack

type protocol struct {
	nsqd *NSQD
}

func (p *protocol) NewClient(conn net.Conn) abstract.Client {
	clientID := atomic.AddInt64(&p.nsqd.clientID, 1)
	return newClient(clientID, conn, p.nsqd)
}

func (p *protocol) IOLoop(c abstract.Client) error {
	var err error
	var line []byte
	var zeroTime time.Time

	client := c.(*client)

	messagePupmStartedChan := make(chan bool)
	go p.messagePupm(client, messagePupmStartedChan)
	//等待messagePupm协程启动
	<-messagePupmStartedChan

	for {
		if client.HeartbeatInterval > 0 {
			client.SetDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetDeadline(zeroTime)
		}
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil //连接关闭
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}
		//去除'\n'
		line = line[:len(line)-1]

		//去除'\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		//"空格切割命令"
		params := bytes.Split(line, separatorBytes)

		mlog.Debug("PROTOCOL: [%s] %s", client, params)

		var response []byte

		//TODO:err的返回需要特别关注
		response, err = p.Exec(client, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				ctx = "-" + parentErr.Error()
			}
			mlog.Error("[%s] - %s%s", client, err, ctx)

			//此类错误需要直接断开连接
			if _, ok := err.(*util.FatalClientErr); ok {
				break
			}
			continue
		}
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("send response  err - %s", err)
				break
			}
		}
	}

	mlog.Info("PROTOCOL: [%s] exiting ioloop", client)
	close(client.ExitChan) //告知messagePmp关闭
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}
	return err

}

func (p *protocol) messagePupm(c *client, startchan chan bool) {

}

// 根据指令执行对应的函数
func (p *protocol) Exec(c *client, params [][]byte) ([]byte, error) {

}

// 发送消息 len+type+data
func (p *protocol) Send(c *client, Type int32, data []byte) error {
	c.writeLock.Lock()

	var zeroTime time.Time

	//设置写超时
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	_, err := abstract.SendFramedResponse(c.Writer, Type, data)
	if err != nil {
		c.writeLock.Unlock()
		return err
	}

	//TypeMessage的flush机制放在messagePump进行保证
	if Type != frameTypeMessage {
		err = c.Flush() //进行一次数据刷新
	}

	c.writeLock.Unlock()
	return err
}
