package server

import (
	"MQ/diskqueue"
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
	c.inFlightMutex.Lock() //TODO :此处不加锁会怎么样
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
		//TODO :特殊初始化
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
func (c *Channel) removeInFlight(msg *Message) error {
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

func (c *Channel) StartDeferredTimout(msg *Message, clienId int64, timeout time.Duration) error {
	outtime := time.Now().Add(timeout).UnixNano()
	item := &Item{Value: msg, Priority: outtime}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addDeferredPQ(item)
	return nil
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

// func (c *Channel) processDeferredQueue(t int64) bool {

// }

// 是否正在exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitflag) == 1
}

// func (c *Channel) Delete() error {

// }
