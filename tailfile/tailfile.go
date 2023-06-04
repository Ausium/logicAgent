package tailfile

import (
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logAgent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path    string
	topic   string
	TailObj *tail.Tail
}

func (t *tailTask) run() {
	//读取日志，发往kafka
	//TailObj --> log --> client --> kafka
	logrus.Infof("collect for path:%s is running!", t.path)
	for {

		//循环读取数据
		line, ok := <-t.TailObj.Lines
		logrus.Infof("%v日志文件读取到的最新日志内容为:%v", t.path, line.Text)
		if !ok {
			logrus.Warn("tail file close reopen filename:%s\n", t.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		//如果line是空行的时候，就可以不往kafka里面发送数据了
		if len(strings.Trim(line.Text, "\r")) == 0 {
			logrus.Infof("出现空行啦，直接跳过！")
			continue
		}
		logrus.Infof("============%v,%v", t.topic, t.path)
		//利用通道将同步的代码改为异步的
		//把读出来的一行日志包装成kafka里面的msg类型，丢到通道中
		msg := &sarama.ProducerMessage{}
		msg.Topic = t.topic
		msg.Value = sarama.StringEncoder(line.Text)
		//丢到通道中
		//kafka.ToMsgChan() <- msg
		kafka.ToMsgChan(msg)
	}
}
