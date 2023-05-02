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

	name                string
	dataPath            string //数据文件存储路径
	maxBytesPerFile     int64  //文件最大长度(预设)
	maxBytesPerFileRead int64  //实际可以读到的文件大小
	minMsgSize          int32  //最小消息长度
	maxMsgSize          int32  //最大消息长度

	syncEvery   int64         //读取阈值，到达阈值需要进行sync
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
	//第一次元数据文件可能不存在
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
	d.RLock() // TODO：为什么这一块是加读锁 : 因为Put是可以并发进行的，但是Put和empty和exit不能并发执行
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

// 载入元数据(加载到缓存)
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	//获取元数据名
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
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
	d.depth = depth
	d.nextReadNum = d.readFileNum
	d.nextReadPos = d.readPos
	return nil
}

// 更新元数据(加载到磁盘)
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
		d.depth,
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

// 清空所有数据
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
		//下面是用来更新可以读到的文件大小
		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err == nil {
				d.maxBytesPerFileRead = stat.Size()
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

	//此时还未发送给消费者(未被读走)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadNum = d.readFileNum
	//文件大小超过设定阈值，自增
	if d.readFileNum < d.writeFileNum && d.nextReadPos >= d.maxBytesPerFileRead {
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
	//消息包大小
	dataLen := int32(len(data))
	totalBytes := int64(4 + dataLen)
	//消息大小无效
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}
	if d.writePos > 0 && d.writePos+totalBytes > d.maxBytesPerFile {
		if d.readFileNum == d.writeFileNum { //如果读的文件正好是刚刚写完的
			d.maxBytesPerFileRead = d.writePos
		}

		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			mlog.Error("DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
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

	//清空的读缓冲
	d.writeBuf.Reset()
	//写到缓冲中
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
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

	d.writePos += totalBytes
	d.depth += 1

	return err
}

// 删除所有文件，重新开始
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			mlog.Error("DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0

	return err
}

func (d *diskQueue) deleteAllFiles() error {
	//删除数据文件
	err := d.skipToNextRWFile()

	//删除元数据文件
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		mlog.Error("DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}
	//此时到达消息队列尾部，消息已经全部读取完成

	//此时应该消息堆积量为零
	if depth != 0 {
		if depth < 0 { //元数据损坏
			mlog.Error("DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...", d.name, depth)
		} else if depth > 0 { //数据丢失
			mlog.Error("DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...", d.name, depth)
		}
		// 强制刷新为0
		d.depth = 0
		d.needSync = true
	}

	//出现错误，read > write
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			mlog.Error("DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			mlog.Error("DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// 往前移动一个数据，并删除已读文件
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadNum
	d.readPos = d.nextReadPos
	d.depth -= 1

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			mlog.Error("DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}
	//检查并恢复
	d.checkTailCorruption(d.depth)
}

// 处理拥有错误数据的文件，改名并跳过
func (d *diskQueue) handleReadError() {
	if d.readFileNum == d.writeFileNum {
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	mlog.Warn("DISKQUEUE(%s) jump to next file and saving bad file as %s", d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		mlog.Error("DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s", d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true

	d.checkTailCorruption(d.depth)
}

// 主要处理逻辑
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64 //读取次数
	var r chan []byte
	var p chan []byte
	//刷新时间
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		//是否到达阈值
		if count == d.syncEvery {
			d.needSync = true
		}
		if d.needSync {
			err = d.sync()
			if err != nil {
				mlog.Error("DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}
		//是否有数据可读
		if d.readFileNum < d.writeFileNum || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					mlog.Error("DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
			p = d.peekChan
		} else { //无数据可读
			r = nil
			p = nil
		}

		select {
		case p <- dataRead:
		case r <- dataRead:
			//如果消息被用户读走，readpos和readfilenum往前移动
			count++
			d.moveForward()
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			//删除所有文件
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}

	}
exit:
	mlog.Info("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()   //关闭定时器，避免内存泄漏
	d.exitSyncChan <- 1 //进行同步，让exit（原本在阻塞等待）继续往下执行嗯
}
