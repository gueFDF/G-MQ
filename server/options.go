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
	ClientTimeout time.Duration
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
		ID:                       defaultID,
		logLevel:                 mlog.INFO,
		logPrefix:                "[nsqd]",
		TCPAddress:               "0.0.0.0:4150",
		HTTPAddress:              "0.0.0.0:4151",
		HTTPSAddress:             "0.0.0.0:4152",
		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,
		MsgTimeout:               60 * time.Second,
		ClientTimeout:            60 * time.Second,
	}
}
