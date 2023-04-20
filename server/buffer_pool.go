package server

//此文件包是bytes.Buffer池子，是一层优化
import (
	"bytes"
	"sync"
)

var bp sync.Pool

// 初始化
func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}
