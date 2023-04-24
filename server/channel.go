package server

import (
	"MQ/diskqueue"
	"MQ/mlog"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type deletecallback func(*Channel)
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats(string) int
	Empty()
}

type Channel struct {
	topicName string //所属topic的name
	name      string //channel的名字
	nsqd      *NSQD

	//message
	requeueCount uint64 //重新投递的消息次数
	messageCount uint64 //topic投递的消息数量s
	timeoutCount uint64 //超时的消息数量
	sync.RWMutex

	backend BackendQueue //持久化机制

	memoryMsgChan chan *Message //缓存

	exitflag  int32        //退出标记
	exitMutex sync.RWMutex //对exitflag的小粒度锁

	clients        map[int64]Consumer //订阅的clients
	paused         int32              //是否暂停
	ephemeral      bool               //是否是临时的channel
	deleteCallback func(*Channel)     //删除的回调函数
	deleter        sync.Once          //保证deleteCallback只执行一次

	//延迟队列
	defferredMessage map[MessageID]*Item //通过map管理
	defferredPQ      DelayPqueue         //优先级队列
	defferredMutex   sync.Mutex          //小粒度锁

	//待确认队列
	inFlightMessage map[MessageID]*Message //map的管理
	inFlightPQ      InFlightPqueue         //优先级队列
	inFlightMutex   sync.Mutex             //小粒度锁
}

// 初始化两个优先级队列
func (c *Channel) initPQ() {
	//两个优先级队列的大小为messageChan的十分之一,且至少为一
	//TODO :为什么size要这样设计
	PQsize := int(c.nsqd.getOpts().MemQueueSize / 10)
	if PQsize == 0 { //至少为1
		PQsize = 1
	}

	//初始化等待确认队列
	c.inFlightMutex.Lock()
	//此处不加锁会怎么样 :重置也会调用该函数，所以要加锁
	c.inFlightMessage = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(PQsize)
	c.inFlightMutex.Unlock()

	//初始化延迟消息队列
	c.defferredMutex.Lock()
	c.defferredMessage = make(map[MessageID]*Item)
	c.defferredPQ = newDelayPqueue(PQsize)
	c.defferredMutex.Unlock()

}

// new channel
func NewChannel(topicName, channelName string, nsqd *NSQD, deleteCallback deletecallback) *Channel {
	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		nsqd:           nsqd,
		ephemeral:      strings.HasSuffix(channelName, "#ephemeral"), //通关channel的前缀判断channel是否是一个临时的channel
	}

	if nsqd.getOpts().MemQueueSize > 0 || c.ephemeral {
		c.memoryMsgChan = make(chan *Message, nsqd.getOpts().MaxMsgSize)
	}

	//初始化优先级队列
	c.initPQ()

	//初始化磁盘队列
	if c.ephemeral {
		c.backend = newFakeBackendQueue()
	} else {
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength), //MsgIDLength + 8 + 2
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
		)
	}
	//TODO :somesing about ephemeral
	return c
}

//下面是关于对inFlight操作的方法

// 向inFlightMessage推送消息
func (c *Channel) pushInFilghtMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessage[msg.ID] //判断该消息是否已经存在
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessage[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// 从inFlinghtMssage删除消息
// 为了当值有client,恶意发送FIN ,加入cientID ,进行保障该消息是该client拥有的
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Unlock()
	msg, ok := c.inFlightMessage[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID { //该消息不属于该客户端，一层安全措施
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessage, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

// 向InFlightPQ推送消息
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 从InFlightPQ remove
func (c *Channel) removeInFlightPQ(msg *Message) error {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		//该消息已经被pop
		c.inFlightMutex.Unlock()
		return errors.New("msg is already been poped")
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
	return nil
}

func (c *Channel) StartInfilghtTimeout(msg *Message, clientId int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientId
	msg.dis_time = now                    //发布时间
	msg.pri = now.Add(timeout).UnixNano() //距离超时的时间
	err := c.pushInFilghtMessage(msg)     //先向InflightMessage推送，确保该消息不在当中
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil

}

// 查询是否有超时的数据
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	//关闭期间不能进行消息发送，因为关闭期间会将缓存中未发送的消息写入磁盘，如果继续进行发送会导致，消息多次消费
	defer c.exitMutex.RUnlock()
	if c.Exiting() {
		return false
	}
	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()
		if msg == nil {
			goto exit
		}
		dirty = true
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1) //过期的消息数量+1
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			// TODO ：通知
			client.TimedOutMessage()
		}
		c.put(msg)
	}
exit:
	return dirty
}

// 下面是关于defferred的操作

// 将item push deferredMessage
func (c *Channel) pushDeferredMessage(item *Item) error {
	c.defferredMutex.Lock()
	id := item.Value.(*Message).ID
	_, ok := c.defferredMessage[id]
	if ok {
		c.defferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.defferredMessage[id] = item
	c.defferredMutex.Unlock()
	return nil
}

// 将item  pop deferredMessage
func (c *Channel) popDefferedMessage(id MessageID) (*Item, error) {
	c.defferredMutex.Lock()
	item, ok := c.defferredMessage[id]
	if !ok {
		c.defferredMutex.Unlock()
		return nil, errors.New("ID id not in deferred")
	}
	delete(c.defferredMessage, id)
	c.defferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addDeferredPQ(item *Item) {
	c.defferredMutex.Lock()
	c.defferredPQ.Push(item)
	c.defferredMutex.Unlock()
}

func (c *Channel) StartDeferredTimout(msg *Message, timeout time.Duration) error {
	outtime := time.Now().Add(timeout).UnixNano()
	item := &Item{Value: msg, Priority: outtime}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addDeferredPQ(item)
	return nil
}

// 查询是否有要进行发送的数据（到期）
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	//关闭期间不能进行消息发送，因为关闭期间会将缓存中未发送的消息写入磁盘，如果继续进行发送会导致，消息多次消费
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}
	dirty := false
	for {
		c.defferredMutex.Lock()
		Item, _ := c.defferredPQ.PeekAndShift(t)
		c.defferredMutex.Unlock()

		if Item == nil { //此时没有消息过期
			goto exit
		}
		dirty = true
		msg := Item.Value.(*Message)
		_, err := c.popDefferedMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}
exit:
	return dirty
}

// 下面是对clients的操作

func (c *Channel) AddClient(clientId int64, client Consumer) error {
	c.exitMutex.RLock() //此期间不允许exiting
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return errors.New("exting")
	}
	c.RLock() //TODO :这里用读写锁进行优化，能带来很多性能提升么
	_, ok := c.clients[clientId]
	numClients := len(c.clients)
	c.Unlock()
	if ok {
		return nil //client 已经存在
	}
	maxChannelConsumers := c.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && numClients >= maxChannelConsumers {
		//连接数量已经到达上限
		return fmt.Errorf("consumers for %s:%s exceeds limit of %d",
			c.topicName, c.name, maxChannelConsumers)
	}
	c.Lock()
	c.clients[clientId] = client
	c.Unlock()
	return nil
}

func (c *Channel) RemoveClient(clientID int64) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return errors.New("exiting")
	}
	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if !ok { //不存在
		return fmt.Errorf("client(%d) is not exist", clientID)
	}
	c.Lock()
	delete(c.clients, clientID)
	c.Unlock()

	if len(c.clients) == 0 && c.ephemeral {
		//如果该channel是临时的，即将被deleted，调删除回调
		go c.deleter.Do(func() {
			c.deleteCallback(c)
		})
	}
	return nil

}

// 是否正在exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitflag) == 1
}

func (c *Channel) Delete() error {
	return c.exit(true)
}
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitflag, 0, 1) {
		//如果已经在exit
		return errors.New("exiting")
	}

	if deleted {
		mlog.Info("CHANNEL(%s): deleting", c.name)
		//TODO :从LOOkUp删除注册
	} else {
		mlog.Info("CHANNEL(%s): closing", c.name)
	}

	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		//清空所有数据包括内存中的
		c.Empty()
		return c.backend.Delete()
	}
	//刷新，将缓存中未发送，未收到ack的消息flush到磁盘当中
	c.flush()

	return c.backend.Close()

}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	//重置两个队列  //TODO :不重置会怎么样
	c.initPQ()

	//重置客户端
	for _, client := range c.clients {
		client.Empty()
	}
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}
finish:
	return c.backend.Empty() //清空所有磁盘文件
}

func (c *Channel) flush() {
	//缓存管道还有数据
	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessage) > 0 || len(c.defferredMessage) > 0 {
		mlog.Info("CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessage), len(c.defferredMessage))
	}
	for { //将messahe中的消息写到磁盘当中
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(msg, c.backend)
			if err != nil {
				mlog.Error("failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}
finish:
	c.inFlightMutex.Lock()
	//将为未收到消息确认的也写入磁盘当中
	for _, msg := range c.inFlightMessage {
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			mlog.Error("failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.defferredMutex.Lock()
	//将延迟消息写入磁盘当中
	for _, item := range c.defferredMessage {
		err := writeMessageToBackend(item.Value.(*Message), c.backend)
		if err != nil {
			mlog.Error("failed to write message to backend - %s", err)
		}
	}
	c.defferredMutex.Unlock()
}

// 返回堆积消息
func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan) + int(c.backend.Depth()))
}

// 暂停
func (c *Channel) Pause() error {
	return c.doPause(true)
}

// 取消暂停
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// 更改状态（暂停/非暂停）
func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1) //改为暂停状态
	} else {
		atomic.StoreInt32(&c.paused, 0) //取消暂停状态
	}

	c.RLock()
	for _, client := range c.clients { //更改client
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

// 判断是否暂停
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

func (c *Channel) PutMessage(m *Message) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1) //发送消息数+1
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default: //缓冲满了，写磁盘
		err := writeMessageToBackend(m, c.backend)
		//TODO :如果此处出现err,需要重视，因为此处可能造成数据丢失
		//TODO :c.nsqd.SetHealth(err)
		if err != nil {
			mlog.Error("CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// 推送延迟消息
func (c *Channel) PutDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimout(msg, timeout)
}

// 重置inflightmessage
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	//从PQremove
	c.removeInFlightPQ(msg)
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.dis_time) >= c.nsqd.getOpts().MaxMsgTimeout {
		//将超时时间设置为最大，减少重发次数
		newTimeout = msg.dis_time.Add(c.nsqd.getOpts().MaxMsgTimeout)
	}
	//重置超时时间
	msg.pri = newTimeout.UnixNano()
	err = c.pushInFilghtMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

//finish

func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return nil
	}

	c.removeInFlightPQ(msg)
	//TODO admin
	return nil
}

// 重发
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeInFlightPQ(msg)
	atomic.AddUint64(&c.requeueCount, 1)

	if timeout == 0 { //不进行延时发送
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}
	return c.StartDeferredTimout(msg, timeout)
}
