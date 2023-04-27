package server

import (
	"MQ/mlog"
	"bufio"
	"compress/flate"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufferSize = 16 * 1024 //默认缓冲区

// client的状态
const (
	stateInit         = iota //待初始化
	stateDisconnected        //断开连接
	stateConnected           //连接
	stateSubscribed          //关注订阅
	stateClosing             //关闭中
)

type client struct {
	ReadyCount    int64
	InFlightCount int64
	MessageCount  uint64
	FinishCount   uint64
	RequeueCount  uint64

	pubCounts map[string]uint64

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID int64

	net.Conn //连接

	//安全鉴权
	//TODO：暂时不添加该功能

	flateWriter *flate.Writer //可进行压缩

	//reader 和  writer的缓冲优化
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration //心跳包周期

	MsgTimeout time.Duration

	State          int32 //状态
	ConnectTime    time.Time
	Channel        *Channel
	ReadyStateChan chan int //readyCount更新后用来进行消息通知
	ExitChan       chan int

	clientId string
	Hostname string

	SubEventChan chan *Channel //绑定的channel

	//两个可重复使用的缓冲区
	lenbuf   [4]byte
	lenSlice []byte

	nsqd *NSQD
}

func newClient(id int64, conn net.Conn, nsqd *NSQD) *client {
	var flag string
	if conn != nil {
		flag, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	c := &client{
		ID:   id,
		nsqd: nsqd,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: nsqd.getOpts().OutputBufferTimeout,

		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		clientId: flag,
		Hostname: flag,

		SubEventChan: make(chan *Channel),

		//心跳周期默认为ClientTimeout的一半 ，30s
		HeartbeatInterval: nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}

	c.lenSlice = c.lenbuf[:]
	return c
}

func (c *client) String() string {
	return c.RemoteAddr().String()
}

// 判断是消费者还是生产者
func (c *client) Type() int {
	c.metaLock.RLock()
	hasPublished := len(c.pubCounts) > 0
	c.metaLock.RUnlock()
	if hasPublished {
		return typeProducer
	}
	return typeProducer
}

// 是否准备好接收消息
func (c *client) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		return false
	}
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	mlog.Debug("[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	//如果未受到ack的消息过多(可能出现网络延迟)，或者没有消息可读，就返回false,不接收消息
	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}
	return true
}

// 设置readyCount
func (c *client) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count)

	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

// 通知更新
func (c *client) tryUpdateReadyState() {
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

// 消息完成
// FIN
func (c *client) FinshedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// 清空(InFlightCount置为0)
func (c *client) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

// SEND
func (c *client) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)

	//TODO? 此处是否需要进行update
}

// PUB
func (c *client) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

// TIMEOUT
func (c *client) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// REQ
func (c *client) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// CLOSE
func (c *client) StartClose() {
	c.SetReadyCount(0)
	atomic.StoreInt32(&c.State, stateClosing)
	//TODO? 此处是否需要进行update
}

// ? 此处目的是什么
func (c *client) Pause() {
	c.tryUpdateReadyState()
}

func (c *client) UnPause() {
	c.tryUpdateReadyState()
}

// 设置心跳
func (c *client) SetHearbeatInterval(interval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case interval == -1: //没有限制
		c.HeartbeatInterval = 0
	case interval == 0:
		//使用默认
	case interval >= 1000 && interval <= int(c.nsqd.getOpts().MaxHeartbeatInterval):
		c.HeartbeatInterval = time.Duration(interval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval(%d) invalid", interval)
	}
	return nil
}

func (c *client) SetOutputBuffer(size int, timeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	//配置OutputBufferTimeout，缓冲区的刷新的时间
	switch {
	case timeout == -1:
		c.OutputBufferSize = 0
	case timeout == 0:
		//使用默认
	case timeout >= int(c.nsqd.getOpts().MinOutputBufferTimeout/time.Millisecond) &&
		timeout <= int(c.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		c.OutputBufferTimeout = time.Duration(timeout) * time.Millisecond
	default:
		return fmt.Errorf("OutputBufferTimeout interval(%d) invalid", timeout)
	}

	//配置buffersize
	switch {
	case size == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		c.OutputBufferSize = 1
		c.OutputBufferTimeout = 0
	case size == 0:
		//使用默认a
	case size >= 64 && size <= int(c.nsqd.getOpts().MaxOutputBufferSize):
		c.OutputBufferSize = size
	default:
		return fmt.Errorf("OutputBufferSize (%d) is invalid", size)
	}

	if size != 0 {
		//刷新原来缓冲区(刷新到远端)，创建新的缓冲区(bufio)
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, c.OutputBufferSize)
	}
	return nil
}

// 设置消息的超时时间
func (c *client) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// 默认配置
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

// 刷新
func (c *client) Flush() error {
	var zeroTime time.Time
	//TODO ?此处的目的，为什么设置为心跳
	//设置写超时
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}
