package util

import (
	"MQ/mlog"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

//此工具包用来生成分布式唯一ID,采用雪花算法

const (
	epoch          = int64(1577808000000)              // 设置起始时间(时间戳/毫秒)：2020-01-01 00:00:00，有效期69年
	timestampBits  = uint(41)                          // 时间戳占用位数
	workeridBits   = uint(10)                          // 机器id所占位数
	sequenceBits   = uint(12)                          // 序列所占的位数
	timestampMax   = int64(-1 ^ (-1 << timestampBits)) // 时间戳最大值
	workeridMax    = int64(-1 ^ (-1 << workeridBits))  // 支持的最大机器id数量
	sequenceMask   = int64(-1 ^ (-1 << sequenceBits))  // 支持的最大序列id数量
	workeridShift  = sequenceBits                      // 机器id左移位数
	timestampShift = sequenceBits + workeridBits       // 时间戳左移位数
)

type Snowflake struct {
	sync.Mutex
	timestamp    int64
	workerid     int64
	datacenterid int64
	sequence     int64
}

// uid
type uid int64

// 生成id工场
func NewSonwflacke(workerid int64) (*Snowflake, error) {
	if workerid < 0 || workerid > workeridMax {
		return nil, fmt.Errorf("workerid must be between 0 and %d", workeridMax-1)
	}
	return &Snowflake{
		timestamp: 0,
		workerid:  workerid,
		sequence:  0,
	}, nil
}

//获取id

func (s *Snowflake) Getid() uid {
	s.Lock()
	now := time.Now().UnixNano() / 1000000 //转毫秒
	if s.timestamp == now {                //同一时间下获取多个id
		s.sequence = (s.sequence + 1) & sequenceMask //如果超过12个bit,与sequenceMask&结果为0
		if s.sequence == 0 {
			//等待下一个好毫秒
			for now <= s.timestamp {
				now = time.Now().UnixNano() / 1000000
			}
		}
	} else {
		s.sequence = 0 //不同时间下，序列号从另零开始
	}

	t := now - epoch
	if t > timestampMax { //超出限制
		s.Unlock()
		mlog.Error("epoch must between 0 and %d", timestampMax-1)
		return 0
	}
	s.timestamp = now

	r := int64(t)<<timestampShift | (s.workerid << int64(workeridShift)) | s.sequence
	s.Unlock()
	return uid(r)

}

// 获取时间戳
func GetTimestamp(sid int64) (timestamp int64) {
	timestamp = (sid >> timestampShift) & timestampMax
	return
}

// 获取创建ID时的时间戳
func GetGenTimestamp(sid int64) (timestamp int64) {
	timestamp = GetTimestamp(sid) + epoch
	return
}

// 获取创建ID时的时间字符串(精度：秒)
func GetGenTime(sid int64) (t string) {
	// 需将GetGenTimestamp获取的时间戳/1000转换成秒
	t = time.Unix(GetGenTimestamp(sid)/1000, 0).Format("2006-01-02 15:04:05")
	return
}

// 获取时间戳已使用的占比：范围（0.0 - 1.0）
func GetTimestampStatus() (state float64) {
	state = float64((time.Now().UnixNano()/1000000 - epoch)) / float64(timestampMax)
	return
}

func (u uid) Tobytes() []byte {
	s := make([]byte, 8)
	d := make([]byte, 16)

	s[0] = byte(u >> 56)
	s[1] = byte(u >> 48)
	s[2] = byte(u >> 40)
	s[3] = byte(u >> 32)
	s[4] = byte(u >> 24)
	s[5] = byte(u >> 16)
	s[6] = byte(u >> 8)
	s[7] = byte(u)
	//进行16进制编码
	hex.Encode(d[:], s[:])
	return d
}
