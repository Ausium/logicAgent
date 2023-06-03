package tailfile

import (
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logAgent/common"
	"logAgent/kafka"
	"time"
)

type tailTask struct {
	path    string
	topic   string
	TailObj *tail.Tail
}

var (
	confChan chan []common.CollectEntry
)

func (t *tailTask) run() {
	//读取日志，发往kafka
	//TailObj --> log --> client --> kafka
	logrus.Infof("collect fopr path:%s is running!", t.path)
	for {
		//循环读取数据
		line, ok := <-t.TailObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen filename:%s\n", t.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		//如果line是空行的时候，就可以不往kafka里面发送数据了
		if len(line.Text) == 0 {
			logrus.Info()
			continue
		}

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

func Init(allConf []common.CollectEntry) (err error) {
	//allConf里面存放了若干个日志的收集项
	//针对每一个日志收集项创建一个对应的tailObj
	config := tail.Config{
		ReOpen:   true,
		Follow:   true,
		Location: &tail.SeekInfo{Offset: 0, Whence: 2},
		Poll:     true,
	}
	for _, conf := range allConf {
		tt := tailTask{
			path:  conf.Path,
			topic: conf.Topic,
		}
		//打开文件开始读取数据
		tt.TailObj, err = tail.TailFile(tt.path, config)
		if err != nil {
			logrus.Errorf("tailfile: create TailObj for path: %s failed, err:", tt.path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%s success!", conf.Path)
		//收集日志
		go tt.run()
	}
	//初始化新配置管道
	confChan = make(chan []common.CollectEntry)
	//派一个小弟等着新配置来
	newConf := <-confChan //取到值说明新的配置来了
	//新配置来了之后就应该管理以下之前启动的tailTask
	logrus.Infof("get new conf from etcd, conf is:%v", newConf)
	return
}

func SendNewConf(newConf []common.CollectEntry) {
	confChan <- newConf
}
