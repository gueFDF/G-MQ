package server

import (
	"MQ/diskqueue"
	"MQ/mlog"
	"MQ/util"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

type Topic struct {
	messageCount uint64 //消息数量
	messageBytes uint64 //消息总字节数

	sync.RWMutex

	name          string
	channelMap    map[string]*Channel //管理所有的channel
	backend       BackendQueue        //磁盘队列
	memoryMsgChan chan *Message       //消息缓存

	//暂停
	paused    int32
	pauseChan chan int
	//下面三个为信号通知管道
	startChan         chan int
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32           //flag
	idFactory         *util.Snowflake //生产唯一uid

	ephemeral      bool //是否是临时
	deleteCallback func(*Topic)
	deleter        sync.Once //保证deleteCallback只执行一次

	nsqd *NSQD
}

// 创建新的topic
func NewTopic(topicName string, nsqd *NSQD, dedeletecallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		nsqd:              nsqd,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    dedeletecallback,
	}
	f, err := util.NewSonwflacke(nsqd.getOpts().ID)
	if err != nil {
		mlog.Error("newSoneflacke err: %s", err.Error())
	}
	t.idFactory = f

	//初始化磁盘
	if strings.HasSuffix(topicName, "#ephemeral") { //临时topic
		t.ephemeral = true
		t.backend = newFakeBackendQueue() //伪磁盘
	} else {
		t.backend = diskqueue.New( //创建磁盘队列
			topicName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength), //MsgIDLength + 8 + 2
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
		)
	}
	//开启 messagePump
	t.waitGroup.Wrap(t.messagePump)
	//TODO : Notify

	return t
}

// 启动
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// 判断是否会关闭
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// 获取一个channel
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreatChannel(channelName)
	t.Unlock()
	if isNew {
		select {
		case t.channelUpdateChan <- 1: //通知更新
		case <-t.exitChan:
		}
	}
	return channel
}

// 返回channel ，如果channel原本存在返回false,如果是新创建的返回true
func (t *Topic) getOrCreatChannel(channelName string) (*Channel, bool) {
	c, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) { //channel delete是调用回调，从topic中的channelmap删除
			t.DeleteExistingChannel(c.name)
		}
		channel := NewChannel(t.name, channelName, t.nsqd, deleteCallback)
		t.channelMap[channelName] = channel
		mlog.Info("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return c, false
}

// 获取存在的Channel
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// 删除一个存在的Channel
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	if !ok {
		return errors.New("channel does not exist")
	}
	mlog.Info("TOPIC(%s): deleting channel %s", t.name, channel.name)
	channel.Delete()

	t.Lock()
	delete(t.channelMap, channelName)
	numChannels := len(t.channelMap)
	t.Unlock()
	select { //通知更新
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	//如果这是一个临时topic,当管道数为0时需要delete
	if numChannels == 0 && t.ephemeral {
		go t.deleter.Do(func() {
			t.deleteCallback(t)
		})
	}
	return nil
}

func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}

	//增加计数
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()

	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotlaBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			//计数
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotlaBytes))
			return err
		}
		messageTotlaBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotlaBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

func (t *Topic) put(m *Message) error {
	//此处加一层是为了提供更强的一致性并
	//延时消息优先进入缓存，因为就爱如磁盘会导致延时无效
	//临时topic优先往缓存中写
	if cap(t.memoryMsgChan) > 0 || t.ephemeral || m.deferred != 0 {
		select {
		case t.memoryMsgChan <- m:
			return nil
		default:
			break
		}
	}
	err := writeMessageToBackend(m, t.backend)
	//TODO ：sethelth
	if err != nil {
		mlog.Error("TOPIC(%s) ERROR: failed to write message to backend - %s",
			t.name, err)
		return err
	}
	return nil
}

// 返回消息堆积数量
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan) + int(t.backend.Depth()))
}
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	//等待开启，并且忽略部分信号
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
	}
	t.RLock()
	//拷贝一份client,后续对拷贝的操作，从而高效的保证了channelMap的并发安全
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	//channel不为0 & 没有暂停
	//此处目的是，如果此时没有消费者，或者该topic已经暂停，就不进行设置，避免造成消息丢失
	//下同
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				mlog.Error("failed to decode message - %s", err)
			}
		case <-t.channelUpdateChan: //需要重新拷贝channels
			chans = chans[:0] //清空
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.Unlock()
			//判断是否需要进行消息消费
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		//case 
		}
	}
exit:
}

func (t *Topic) IsPaused() bool {
	//memoryMsgChan=nil
	return true
}
