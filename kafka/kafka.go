package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// 这里暴露全局变量，其他地方也可以使用他
var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

//kafka相关操作

// Init 是初始化全局的kafka Client
func Init(address []string, chanSize int64) (err error) {
	//生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区
	config.Producer.Return.Successes = true                   //交付确认

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka: producer closed, err:", err)
		return
	}
	logrus.Infof("kafka连接成功！")
	//初始化MsgChan
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	//起一个后台的goroutine从msgchan中读取数据
	go sendMsg()
	return
}

// 从MsgChan中读取吗msg，发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			logrus.Infof("ready login kafka success")
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg kafka failed, err: v%", err)
				return
			}
			logrus.Infof("send msg to kafka success. pid:%v offset:%v", pid, offset)
		}

	}
}

// 定义一个函数向外暴露msgchan
//
//	func ToMsgChan() chan<- *sarama.ProducerMessage {
//		return msgChan
//	}
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
