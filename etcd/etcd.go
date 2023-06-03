package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"logAgent/common"
	"logAgent/tailfile"
	"time"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

// 拉取日志收集配置项函数
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed, err:%v", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 from etcd by key:%s", key)
		return
	}
	ret := resp.Kvs[0]
	//ret.Value  json格式化
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json Unmarshal failed, err:%v", err)
		return
	}
	return
}

func WatchConf(key string) {
	watchCh := client.Watch(context.Background(), key)
	var newConf []common.CollectEntry
	for wresp := range watchCh {
		logrus.Infof("get new conf from etcd!")
		for _, ev := range wresp.Events {
			fmt.Printf("Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			err := json.Unmarshal(ev.Kv.Value, &newConf)
			if err != nil {
				logrus.Errorf("json Unmarshal new conf failed, err: %v", err)
			}
		}
		//告诉tailfile这个模块应该启用新的配置项了
		tailfile.SendNewConf(newConf) //没有人接收就是阻塞了
	}
}
