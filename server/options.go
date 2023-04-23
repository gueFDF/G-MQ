package server

import (
	"MQ/mlog"
	"crypto/md5"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type Options struct {
	ID        int64      //ID
	logLevel  mlog.Level //日志等级
	logPrefix string     //日志前缀

	TCPAddress               string
	HTTPAddress              string
	HTTPSAddress             string
	HTTPClientConnectTimeout time.Duration //http连接超时时间
	HTTPClientRequestTimeout time.Duration //http请求超时时间

	// msg and command options
	MsgTimeout    time.Duration //消息的超时时间
	MaxMsgTimeout time.Duration //消息的最长超时时间，如果超过则将会将该消息的超时时间设置这个，减少重发次数
	MaxMsgSize    int64         //消息的最大大小
	MaxBodySize   int64
	ClientTimeout time.Duration

	MaxChannelConsumers int //channel的最大订阅数 （0表示没有限制）

	// diskqueue options
	DataPath        string        //磁盘文件存储路径
	MemQueueSize    int64         //msgChan的缓冲大小
	MaxBytesPerFile int64         //磁盘文件的最大大小
	SyncEvery       int64         //读写多少次进行一次刷新
	SyncTimeout     time.Duration //多长时间进行一次刷新
}

func NewOptions() *Options {
	//生成默认的唯一ID
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		logLevel:  mlog.INFO,
		logPrefix: "[nsqd]",

		TCPAddress:               "0.0.0.0:4150",
		HTTPAddress:              "0.0.0.0:4151",
		HTTPSAddress:             "0.0.0.0:4152",
		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		ClientTimeout: 60 * time.Second,

		MaxChannelConsumers: 0,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,
	}
}
