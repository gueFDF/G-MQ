package util

import (
	"fmt"
	"os"
	"syscall"
)

//此文件，封装目录锁

/*
go中的文件锁
syscall.Flock(fd int , how int)函数进行枷锁

how:
syscall.LOCK_NB :获取的是排他锁(加锁后只能自己使用)
syscall.LOCK_SH :获取的是共享锁(加锁后共享锁可以访问，排他不可以访问)
syscall.LOCK_UN :解锁

syscall.LOCK_NB :表示当前获取锁的模式是非阻塞模式，如果需要阻塞模式，不加这个参数即可(默认是阻塞)
*/

type DirLock struct {
	dir string
	f   *os.File
}

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}

}

// 加锁
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s (in use by other)", l.dir, err)
	}
	return nil
}

// 解锁
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
