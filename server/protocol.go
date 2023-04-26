package server

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


