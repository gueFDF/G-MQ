package server

// 一个伪磁盘队列，只是为了适配临时channel，实际上实现都是空接口，当缓冲满了，将消息直接丢弃
type FakeBackendQueue struct {
	readChan chan []byte
}

func newFakeBackendQueue() *FakeBackendQueue {
	return &FakeBackendQueue{readChan: make(chan []byte)}
}

func (f *FakeBackendQueue) Put([]byte) error {
	return nil
}

func (f *FakeBackendQueue) ReadChan() <-chan []byte {
	return f.readChan
}

func (f *FakeBackendQueue) Close() error {
	return nil
}
func (f *FakeBackendQueue) Delete() error {
	return nil
}

func (f *FakeBackendQueue) Depth() int64 {
	return 0
}

func (f *FakeBackendQueue) Empty() error {
	return nil
}
