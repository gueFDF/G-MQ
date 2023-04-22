package server

import "MQ/abstract"

//此文件使用优先级队列实现一个延迟消息队列

type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

func (i *Item) Getpri() int64 {
	return i.Priority
}

func (i *Item) SetIndex(index int) {
	i.Index = index
}

type DelayPqueue struct {
	pqueue abstract.Priority_Queue
}

func newDelayPqueue(capacity int) DelayPqueue {
	return DelayPqueue{
		pqueue: abstract.NewQueue(capacity),
	}
}

func (pq *DelayPqueue) Push(m *Item) {
	pq.pqueue.Push(m)
}

func (pq *DelayPqueue) Pop() *Item {
	return pq.pqueue.Pop().(*Item)
}

func (pq *DelayPqueue) Remove(i int) *Item {
	return pq.pqueue.Remove(i).(*Item)
}

func (pq *DelayPqueue) PeekAndShift(max int64) (*Item, int64) {
	v, e := pq.pqueue.PeekAndShift(max)

	return v.(*Item), e
}

// 下面方法用于测试
func (pq *DelayPqueue) len() int {
	return len(pq.pqueue)
}

func (pq *DelayPqueue) cap() int {
	return cap(pq.pqueue)
}
