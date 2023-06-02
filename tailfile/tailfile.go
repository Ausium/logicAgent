package tailfile

import (
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

var (
	TailObj *tail.Tail
)

func Init(filename string) (err error) {
	config := tail.Config{
		ReOpen:   true,
		Follow:   true,
		Location: &tail.SeekInfo{Offset: 0, Whence: 2},
		Poll:     true,
	}
	//打开文件开始读取数据
	TailObj, err = tail.TailFile(filename, config)
	if err != nil {
		logrus.Error("tailfile: create TailObj for path: %s failed, err:", filename, err)
		return
	}
	return
}
