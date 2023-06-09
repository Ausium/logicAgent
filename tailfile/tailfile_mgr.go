package tailfile

import (
	"github.com/sirupsen/logrus"
	"logAgent/common"
)

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       //所有tailTask的任务
	CollectEntryList []common.CollectEntry      //所有配置项
	confChan         chan []common.CollectEntry //等待新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

// main函数中调用
func Init(allConf []common.CollectEntry) (err error) {
	//allConf里面存放了若干个日志的收集项
	//针对每一个日志收集项创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		CollectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		logrus.Infof("INIT allconf 文件%v,%v", conf.Path, conf.Topic)
		err = tt.Init()
		if err != nil {
			logrus.Errorf("tailfile: create TailObj for path: %s failed, err:", tt.path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%s success!", conf.Path)
		//把创建的这个tailTask任务登录起来
		ttMgr.tailTaskMap[tt.path] = tt
		// 起一个后台goroutine去收集日志
		go tt.run()

	}
	go ttMgr.watch()
	return
}

// 一直在等confChan有值，有值就开始去管理之前的tailTask
func (t *tailTaskMgr) watch() {
	for {
		//派一个小弟等着新配置来
		newConf := <-t.confChan //取到值说明新的配置来了
		//新配置来了之后就应该管理以下之前启动的tailTask
		logrus.Infof("get new conf from etcd, conf is:%v,start manage tailTask...", newConf)
		for _, conf := range newConf {
			//1.原来已经存在的任务就不用动
			if t.isExist(conf) {
				continue
			}
			//2.原来没有的就需要新创建一个tailTask任务
			tt := newTailTask(conf.Path, conf.Topic)
			err := tt.Init()
			if err != nil {
				logrus.Errorf("tailfile: create TailObj for path: %s failed, err:", tt.path, err)
				continue
			}
			logrus.Infof("create a tail task for path:%s success!", conf.Path)
			//把创建的这个tailTask任务登记起来
			ttMgr.tailTaskMap[tt.path] = tt
			// 起一个后台goroutine去收集日志
			go tt.run()
		}
		//3.原来有的现在没有的就需要tailTask停掉
		//找出tailTaskMap中存在，但是newConf中不存在的那些tailTask，把他们停掉
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
				}
			}
			if !found {
				delete(t.tailTaskMap, key) //从管理类中删掉
				task.cancel()
			}
		}
	}
}

// 判断tailTaskMap中是否有收集项
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

// 把新的配置丢到管理对象的confChan中
func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
