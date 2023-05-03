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

		response, err = p.Exec(client, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				ctx = "-" + parentErr.Error()
			}
			mlog.Error("[%s] - %s%s", client, err, ctx)

			//将err返回给client
			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				mlog.Error("[%s] - %s%s", client, sendErr, ctx)
				break
			}
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
	// TODO 进行identify tls
	switch {

	}

	// 无效的指令
	//TODO:此处
	return nil, util.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %s", params[0]))
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

// 发送message
func (p *protocol) SendMessage(client *client, msg *Message) error {
	mlog.Debug("PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (p *protocol) messagePump(c *client, startChan chan bool) {
	var err error
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte
	var subChannel *Channel //绑定的channel

	var flusherChan <-chan time.Time

	subEventChan := c.SubEventChan
	outputBufferTicker := time.NewTicker(c.OutputBufferTimeout)
	heartbeatTicker := time.NewTicker(c.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	MsgTimeout := c.MsgTimeout

	flushed := true

	//通知父协程，messagePump已经启动
	close(startChan)

	for {
		if subChannel == nil || !c.IsReadyForMessages() {
			//不具备接受消息的能力
			//置空，防止在不具备接受消息能力的时候触发，导致压力过大或者消息丢失
			memoryMsgChan = nil
			backendChan = nil
			flusherChan = nil

			//强制进行一次刷新，保证状态切换时缓时buffio中的数据flush
			c.writeLock.Lock()
			err = c.Flush()
			c.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			memoryMsgChan = subChannel.memoryMsgChan
			backendChan = subChannel.backend.ReadChan()
			flusherChan = nil
		} else {
			memoryMsgChan = subChannel.memoryMsgChan
			backendChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C
		}

		select {
		case <-flusherChan:
			//flush
			c.writeLock.Lock()
			err = c.Flush()
			c.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-c.ReadyStateChan: //客户端状态更新时触发，使进入下一次循环
		case subChannel = <-subEventChan:
			subChannel = nil //只绑定一次
		case <-heartbeatChan: //心跳
			err = p.Send(c, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		case b := <-backendChan:
			msg, err := decodeMessage(b)
			if err != nil {
				mlog.Error("failed to decode message - %s", err)
				continue
			}
			msg.Attempts++
			subChannel.StartInfilghtTimeout(msg, msg.clientID, MsgTimeout)
			c.SendingMessage()
			err = p.SendMessage(c, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case msg := <-memoryMsgChan:
			msg.Attempts++

			subChannel.StartInfilghtTimeout(msg, c.ID, MsgTimeout)
			c.SendingMessage()
			err = p.SendMessage(c, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-c.ExitChan:
			goto exit
		}

	}
exit:
	mlog.Info("PROTOCOL: [%s] exiting messagePump", c)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		mlog.Error("PROTOCOL: [%s] messagePump error - %s", c, err)
	}
}

// 绑定 SUB topicname channelname
func (p *protocol) SUB(client *client, params [][]byte) ([]byte, error) {
	//状态验证
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, util.NewFatalClientErr(nil, E_INVALID, "cannot SUB in current state")
	}
	//配置验证
	if client.HeartbeatInterval <= 0 {
		return nil, util.NewFatalClientErr(nil, E_INVALID, "cannot SUB with heartbeats disabled")
	}
	//确保三个参数
	if len(params) < 3 {
		return nil, util.NewFatalClientErr(nil, E_INVALID, "SUB insufficient number of parameters")
	}
	//topicname符合要求
	topicName := string(params[1])
	if IsValidTopicName(topicName) {
		return nil, util.NewFatalClientErr(nil, E_BAD_TOPIC,
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}
	//channelname符合要求
	channelname := string(params[2])
	if IsValidChannelName(channelname) {
		return nil, util.NewFatalClientErr(nil, E_BAD_CHANNEL,
			fmt.Sprintf("SUB channel name %q is not valid", channelname))
	}

	var channel *Channel

	for i := 1; ; i++ {
		topic := p.nsqd.getTopic(topicName)
		channel := topic.GetChannel(channelname)
		if err := channel.AddClient(client.ID, client); err != nil {
			return nil, util.NewFatalClientErr(err, E_SUB_FAILED, "SUB failed "+err.Error())
		}

		//如果此时channel或topic正在关闭，等待关闭
		//TODO :此处干了什么
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			channel.RemoveClient(client.ID)
			if i < 2 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, util.NewFatalClientErr(nil, E_SUB_FAILED, "SUB failed to deleted topic/channel")
		}
		break
	}
	//改变状态，已绑定
	atomic.StoreInt32(&client.State, stateSubscribed)

	client.SubEventChan <- channel

	return okBytes, nil
}
