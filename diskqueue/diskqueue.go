package diskqueue

import (
	"MQ/mlog"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// 此文件实现磁盘队列，用于实现数据持久化
type Interface interface {
	Put([]byte) error        //写数据
	ReadChan() <-chan []byte //读取文件数据，返回chan,可用于多消费者并发读写
	PeekChan() <-chan []byte
	Close() error  //正常关闭，保存元数据
	Delete() error //删除该文件队列，全部退出
	Depth() int64  //返回未读消息积压量
	Empty() error  //清空消息，关闭文件
}

type diskQueue struct {
	readPos      int64 //文件读写位置
	writePos     int64 //文件写入位置
	readFileNum  int64 //读取文件的编号
	writeFileNum int64 //写入文件的编号
	depth        int64 //消息积压量
	sync.RWMutex       //读写锁

	name            string
	dataPath        string //数据文件存储路径
	maxBytesPerFile int64  //文件最大长度
	minMsgSize      int32  //最小消息长度
	maxMsgSize      int32  //最大消息长度

	syncEvery   int64         //文件同步累积次数
	syncTimeout time.Duration //同步定时
	exitFlag    int32         //退出标志位
	needSync    bool          //是否需要同步

	nextReadPos int64 //下一次的读取位置
	nextReadNum int64 //下一次去读取的文件编号

	readFile  *os.File //读取文件
	writeFile *os.File //写入文件

	reader   *bufio.Reader //读缓冲bufio.NewReader(d.readFile)
	writeBuf bytes.Buffer  //写缓冲

	readChan chan []byte //读取的数据,可以多消费者进行通信消息消费
	peekChan chan []byte

	writeChan         chan []byte   //写入通道
	writeResponseChan chan error    //写入接管返回通道
	emptyChan         chan struct{} //通知清空
	emptyResponseChan chan error    //清空反馈通道
	exitChan          chan struct{} // 结束信号通道
	exitSyncChan      chan int      // 退出同步通道
	depthChan         chan int64
}

func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		peekChan:          make(chan []byte),
		depthChan:         make(chan int64),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan struct{}),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan struct{}),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		mlog.Error("DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	return &d
}

// 生成元数据存储文件
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

// 生成文件名
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// 返回队列中的堆积数据
func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthChan
	if !ok {
		// ioLoop exited
		depth = d.depth
	}
	return depth
}

// 返回只读readchan
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// 返回只读peekchan
func (d *diskQueue) PeekChan() <-chan []byte {
	return d.peekChan
}

// 推送消息
func (d *diskQueue) Put(data []byte) error {
	d.RLock() // TODO：为什么这一块是加读锁
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// 正常关闭
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}

	return d.sync()
}

// 全部关闭，不保存元数据
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

// close/delete
func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		mlog.Info("DISKQUEUE(%s): deleting", d.name)
	} else {
		mlog.Info("DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan) //出发IOLOOP的退出逻辑
	// 确保此刻IOLOOP已经退出
	//因为如果此时将下列内容关闭，IOLOOP,可能正在读写该内容，造成段错误或错误触发
	<-d.exitSyncChan

	close(d.depthChan)

	if d.readFile != nil { //关闭读文件
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil { //关闭写文件
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// 持久化元数据
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	//获取元数据名
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,          //待读取消息
		&d.readFileNum,  //待读取文件编号
		&d.readPos,      //read偏移量
		&d.writeFileNum, //待写文件编号
		&d.writePos)     //write偏移量

	if err != nil {
		return err
	}
	//原子更新未读消息
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadNum = d.readFileNum
	d.nextReadPos = d.readPos
	return nil
}

// 同步元数据
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error
	//获取文件名
	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d", fileName, rand.Int())

	//采用一个类似于COW的技巧，先在副本文件进行修改，修改
	//成功后再将原文件进行覆盖
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth), //原子获取
		d.readFileNum,
		d.readPos,
		d.writeFileNum,
		d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()  //刷新缓冲区
	f.Close() //关闭文件

	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) ioLoop() {

}

// 刷新磁盘保证数据全部写入，关闭文件，并保存元数据
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync() //刷新磁盘
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	//持久化
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// 清空
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()
	if d.exitFlag == 1 {
		return errors.New("exiting...")
	}
	d.emptyChan <- struct{}{}
	mlog.Info("DISKQUEUE(%s): emptying", d.name)

	return <-d.emptyResponseChan
}

func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32
	if d.readFile == nil {
		curFilename := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFilename, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		mlog.Info("DISKQUEUE(%s): readOne() opened %s", d.name, curFilename)
		//根据readpos进行文件偏移
		if d.readPos > 0 {
			_, err := d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	//总长度
	totalBytes := int64(4 + msgSize)

	//此时还未发送给消费者
	d.nextReadPos += d.readPos + totalBytes
	d.nextReadNum = d.readFileNum
	//文件大小超过设定阈值，自增
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadNum++
		d.nextReadPos = 0
	}
	return readBuf, nil

}

func (d *diskQueue) writeOne(data []byte) error {
	var err error
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		mlog.Info("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)
		//进行文件偏移
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	//消息包大小
	dataLen := int32(len(data))
	//消息大小无效
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	//清空的读缓冲
	d.writeBuf.Reset()
	//写到缓冲中
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}
	//写到文件
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	// 写入的文件大于切片大小, 则新建文件
	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		// 将之前的文件同步到磁盘
		err = d.sync()
		if err != nil {
			mlog.Error("DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return err
}
