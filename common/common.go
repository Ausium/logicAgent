package common

// etcd日志收集的结构体，单独写出来是因为其他文件也有使用到这个结构体
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}
