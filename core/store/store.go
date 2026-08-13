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
	
	// --- 新增：Misfire 漏发补偿机制的核心字段 ---
	NextTriggerTime int64  `json:"nextTriggerTime"` // 预计的下一次执行时间戳(Unix秒)。用于宕机重启后的漏发比对
	// TODO: [架构缺陷 6] Misfire 补偿策略可配置化
	// 当前默认写死为 "FIRE_ONCE_NOW" (立即补偿一次)。
	// 未来应支持在前端界面选择 "DO_NOTHING"(忽略) 或 "FIRE_ALL_MISSED"(补齐所有漏掉的次数)。
	MisfireStrategy string `json:"misfireStrategy"` 
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
	// [架构修复 1] 返回值多带一个 int64 = etcd 全局 Revision，
	//   供 Leader 做 read-then-watch (ListJobs(rev) + WatchJobs(rev+1)) 统一消费增量。
	//   ⚠️ WatchJobs 故意不进 Store 接口：它返回 clientv3.WatchChan，会把这个接口
	//   强耦合到 etcd 的具体实现，破坏"底层可替换"的抽象。
	ListJobs(ctx context.Context) ([]*JobInfo, int64, error)

	// DeleteJob 删除任务
	DeleteJob(ctx context.Context, id string) error
}
