package store

import (
	"context"
)

// JobInfo 任务元数据 (存 MySQL nanojob_job 表)
type JobInfo struct {
	ID              int64  `json:"id"`              // MySQL 自增, 天然全局唯一 (替代雪花 + WorkerID 池)
	Cron            string `json:"cron"`            // 触发规则, 如 "0/10 * * * * ?"
	ExecutorHandler string `json:"executorHandler"` // 绑定的 Java 端 @XxlJob 名字
	AppName         string `json:"appName"`         // 归属的业务线 (如 loan-service)
	NextTriggerTime int64  `json:"nextTriggerTime"` // 预计的下一次执行时间戳 (Unix秒), 持久化便于重启/故障转移恢复
}

// JobLog 单次执行日志 (存 MySQL nanojob_log 表)
// 触发前先插入一行 (运行中), Java 执行器跑完通过 /api/callback 回填最终结果。
type JobLog struct {
	ID              int64  `json:"id"`              // 自增主键, 同时就是传给 RunReq.LogID 的调度日志 ID
	JobID           int64  `json:"jobId"`           // 任务 ID
	AppName         string `json:"appName"`         // 执行器分组
	ExecutorHandler string `json:"executorHandler"` // 执行器 handler
	ExecID          string `json:"execId"`          // jobID:slot, 确定性执行 ID
	TriggerTime     int64  `json:"triggerTime"`     // 触发时间 (Unix秒)
	TriggerIP       string `json:"triggerIp"`       // 被派发的执行器地址
	HandleCode      int    `json:"handleCode"`      // 0=运行中 / 200=成功 / 500=失败
	HandleMsg       string `json:"handleMsg"`       // 回调带回的日志内容
	CallbackTime    int64  `json:"callbackTime"`    // 回调时间 (Unix秒)
}

// Store 持久化层接口, 底层可替换 (当前实现为 MySQL)
type Store interface {
	// CreateJob 新增任务, 返回 MySQL 自增 ID (写入后同步写回 job.ID)
	CreateJob(ctx context.Context, job *JobInfo) (int64, error)

	// SaveJob 更新任务 (如写回计算好的 NextTriggerTime)
	SaveJob(ctx context.Context, job *JobInfo) error

	// GetJob 根据 ID 查任务详情
	GetJob(ctx context.Context, id int64) (*JobInfo, error)

	// ListJobs 引擎重启/故障转移时全量捞出任务, 塞进时间轮
	ListJobs(ctx context.Context) ([]*JobInfo, error)

	// DeleteJob 删除任务
	DeleteJob(ctx context.Context, id int64) error

	// ---- 执行日志 ----

	// CreateLog 触发前插入一行 (handle_code=0 运行中), 返回 logId
	CreateLog(ctx context.Context, log *JobLog) (int64, error)

	// UpdateLog 回调回填结果, 按 log_id 幂等 (重复回调覆盖即可)
	UpdateLog(ctx context.Context, logID int64, handleCode int, handleMsg string) error

	// ListLogs 查某个任务最近的执行日志
	ListLogs(ctx context.Context, jobID int64) ([]*JobLog, error)
}
