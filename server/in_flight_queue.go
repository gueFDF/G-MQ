package server

import "MQ/abstract"

//此文件使用优先级队列实现一个发送中消息队列

type InFlightPqueue struct {
	pqueue abstract.Priority_Queue
}

func newInFlightPqueue(capacity int) InFlightPqueue {
	return InFlightPqueue{
		pqueue: abstract.NewQueue(capacity),
	}
}

func (pq *InFlightPqueue) Push(m *Message) {
	pq.pqueue.Push(m)
}

func (pq *InFlightPqueue) Pop() *Message {
	return pq.pqueue.Pop().(*Message)
}

func (pq *InFlightPqueue) Remove(i int) *Message {
	return pq.pqueue.Remove(i).(*Message)
}

func (pq *InFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	v, e := pq.pqueue.PeekAndShift(max)

	return v.(*Message), e
}

// 下面方法用于测试
func (pq *InFlightPqueue) len() int {
	return len(pq.pqueue)
}

func (pq *InFlightPqueue) cap() int {
	return cap(pq.pqueue)
}
