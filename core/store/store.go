package store

import (
	"context"
)

// JobInfo 这是要存进数据库里的“任务元数据”模板
type JobInfo struct {
	ID              string `json:"id"`              // 任务唯一ID
	Cron            string `json:"cron"`            // 触发规则，比如 "0/10 * * * * ?"
	ExecutorHandler string `json:"executorHandler"` // 绑定的 Java 端 @XxlJob 名字
	AppName         string `json:"appName"`         // 归属的业务线 (如 loan-service)
	Strategy        string `json:"strategy"`        // 路由策略 (如 SHARDING)
}

// Store 持久化层核心接口
// 无论底层用的是 etcd、MySQL 还是本地文件，都必须严格实现这 4 个方法
type Store interface {
	// SaveJob 保存或修改一个任务配置
	SaveJob(ctx context.Context, job *JobInfo) error

	// GetJob 根据 ID 查任务详情
	GetJob(ctx context.Context, id string) (*JobInfo, error)

	// ListJobs 获取当前系统的所有任务！
	// (核心：Go 引擎每次重启、或者发生故障转移时，都要调这个方法把数据库里的任务全捞出来，塞进时间轮)
	ListJobs(ctx context.Context) ([]*JobInfo, error)

	// DeleteJob 删除任务
	DeleteJob(ctx context.Context, id string) error
}
