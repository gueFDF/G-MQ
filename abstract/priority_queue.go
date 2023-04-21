package abstract

//此文件用来实现一个优先级队列

/*

parent(i)   =  (i-1)/2
left_child  =  2*i+1
right_child =  2*i+2

*/

// 小堆顶

type Value interface {
	Getpri() int64
	SetIndex(index int)
}

type Priority_Queue []Value

func NewQueue(capacity int) Priority_Queue {
	return make(Priority_Queue, 0, capacity)
}

func (q Priority_Queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].SetIndex(i)
	q[j].SetIndex(j)
}

func (q *Priority_Queue) Push(x Value) {
	n := len(*q)
	c := cap(*q)
	if n+1 > c { //需要扩容,扩容的是cap
		new_q := make(Priority_Queue, n, c*2)
		copy(new_q, *q)
		*q = new_q
	}
	//len向右移动一位，给x预留的空间
	*q = (*q)[0 : n+1]
	(*q)[n] = x
	x.SetIndex(n)
	q.up(n)
}

func (q *Priority_Queue) Pop() Value {
	n := len(*q)
	c := cap(*q)
	q.Swap(0, n-1)           //与最后一个交换
	q.down(0, n-1)           //向下浮动
	if n < (c/2) && c > 25 { //需要缩容
		new_q := make(Priority_Queue, n, c/2)
		copy(new_q, *q)
		*q = new_q
	}

	x := (*q)[n-1]     //获取最后一个
	x.SetIndex(-1)     //重置index
	*q = (*q)[0 : n-1] //len向左移动删除最后一个

	return x
}

func (q *Priority_Queue) Remove(i int) Value {
	n := len(*q)
	if n-1 != i {
		q.Swap(i, n-1)
		q.down(i, n-1)
		q.up(i)
	}

	x := (*q)[n-1]
	x.SetIndex(-1)
	*q = (*q)[0 : n-1]
	return x
}


// 与第一个比较优先级，如果优先级大于第一个，则pop第一个
func (q *Priority_Queue) PeekAndShift(max int64) (Value, int64) {
	if len(*q) == 0 {
		return nil, 0
	}
	x := (*q)[0]
	if x.Getpri() > max {
		return nil, x.Getpri() - max
	}
	q.Pop()

	return x, 0
}

// 向上浮动
func (q *Priority_Queue) up(child int) {
	for {
		parent := (child - 1) / 2
		if parent == child || (*q)[child].Getpri() >= (*q)[parent].Getpri() {
			break
		}
		q.Swap(parent, child)
		child = parent
	}

}

// 向下移动
func (q *Priority_Queue) down(parent, l int) {
	for {
		left_child := 2*parent + 1
		//>=l ,超出长度限制，<0 是int溢出
		if left_child >= l || left_child < 0 {
			break
		}
		child := left_child
		right_child := left_child + 1
		if right_child < l && (*q)[right_child].Getpri() < (*q)[left_child].Getpri() {
			child = right_child
		}
		if (*q)[child].Getpri() >= (*q)[parent].Getpri() {
			break
		}
		q.Swap(child, parent)
		parent = child
	}
}

