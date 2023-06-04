package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logAgent/etcd"
	"logAgent/kafka"
	"logAgent/tailfile"
)

//日志收集客户端
//类似的开源项目: filebeat
//收集指定目录下的日志文件，发送到kafka中

//现在的技能包  往kafka发送数据   使用tail读日志文件

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() {
	for {
		select {}
	}
}

func main() {
	//前置工作:初始化
	//1.读取配置文件 'go.ini'
	//cfg, err := ini.Load("./conf/config.ini")
	var configObj = new(Config)
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed, err: v%", err)
		return
	}
	//kafkaAddr := cfg.Section("kafka").Key("address").String()
	fmt.Printf("%#v\n", configObj)
	//2. 初始化连接kafka（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed, err: v%", err)
		return
	}
	logrus.Info("init kafka success!")

	//2.初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err: v%", err)
		return
	}
	logrus.Infof("connect to etcd success!")

	//3.从etcd中拉取要收集的日志配置项
	fmt.Println(configObj.EtcdConfig.CollectKey)
	allConf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err: v%", err)
		return
	}
	//使用一个goroputine去监控etcd中，configObj.EtcdConfig.CollectKey对应值的变化
	//如果值有更新，就去更新etcd的配置项
	go etcd.WatchConf(configObj.EtcdConfig.CollectKey)

	fmt.Println(allConf)
	//4.根据配置中的日志路径初始化tail
	err = tailfile.Init(allConf)
	if err != nil {
		logrus.Errorf("init tailfile failed, err: v%", err)
		return
	}
	logrus.Info("init tailfile success!")
	if err != nil {
		logrus.Errorf("run failed, err: v%", err)
		return
	}
	run()
	//3.把日志通过sarama发往kafka
}
