package server

import "regexp"

// 此文件是用来检测topic 和 channel 的name 是否合理

// 正则表达式（数字，字符，下划线，也可以#ephemeral结尾）
var validTopicChannelNameRegex = regexp.MustCompile(`^[.a-zA-Z0-9_-]+(#ephemeral)?$`)

//有效的topicname
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

//有效的channelname
func IsValidChannelName(name string) bool {
	return isValidName(name)
}


func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
