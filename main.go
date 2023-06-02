package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logAgent/kafka"
	"logAgent/tailfile"
	"time"
)

//日志收集客户端
//类似的开源项目: filebeat
//收集指定目录下的日志文件，发送到kafka中

//现在的技能包  往kafka发送数据   使用tail读日志文件

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

// 真正的业务逻辑
func run() (err error) {
	//TailObj --> log --> client --> kafka
	for {
		//循环读取数据
		line, ok := <-tailfile.TailObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen filename:%s\n", tailfile.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		//利用通道将同步的代码改为异步的
		//把读出来的一行日志包装成kafka里面的msg类型，丢到通道中
		msg := &sarama.ProducerMessage{}
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)
		//丢到通道中
		kafka.MsgChan <- msg
	}

}

func main() {
	//前置工作:初始化
	//1.读取配置文件 'go.ini'
	//cfg, err := ini.Load("./conf/config.ini")
	var configObj = new(Config)
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load config failed, err: v%", err)
		return
	}
	//kafkaAddr := cfg.Section("kafka").Key("address").String()
	fmt.Printf("%#v\n", configObj)
	//2. 初始化连接kafka（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed, err: v%", err)
		return
	}
	logrus.Info("init kafka success!")

	//2.根据配置中的日志路径初始化tail
	err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	if err != nil {
		logrus.Error("init tailfile failed, err: v%", err)
		return
	}
	logrus.Info("init tailfile success!")

	err = run()
	if err != nil {
		logrus.Error("run failed, err: v%", err)
		return
	}
	//3.把日志通过sarama发往kafka
}
