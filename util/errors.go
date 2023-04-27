package util

//此文件对error处理的封装,用来返回一个用户可读的err

type ChildErr interface {
	Parent() error
}

type ClientErr struct {
	ParentErr error
	Code      string //错误指令
	Desc      string //错误描述
}

func (e *ClientErr) Error() string {
	return e.Code + " " + e.Desc
}

func (e *ClientErr) Parent() error {
	return e.ParentErr
}

func NewClientErr(parent error, code string, description string) *ClientErr {
	return &ClientErr{parent, code, description}
}

type FatalClientErr struct {
	ParentErr error
	Code      string
	Desc      string
}

func (e *FatalClientErr) Error() string {
	return e.Code + " " + e.Desc
}

func (e *FatalClientErr) Parent() error {
	return e.ParentErr
}

func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
	return &FatalClientErr{parent, code, description}
}
