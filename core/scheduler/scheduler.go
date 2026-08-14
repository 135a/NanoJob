package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"nanojob/adapter/xxljob"
	"nanojob/core/parser"
	"nanojob/core/registry"
	"nanojob/core/router"
	"nanojob/core/store"
	"nanojob/core/timewheel"
)

// Scheduler 调度核心: 把任务装进时间轮, 到点派发并落执行日志。
// 只在 Leader 上运行 —— 由外部按选举结果调用 Start/Stop。
//
// 相比旧版的 etcd-Watch 统一消费:
//   - 写请求收敛到 Leader 后, 新增任务由本进程直接挂轮子, 不再有"写回→自消费"回环,
//     因此 inWheel/lastFired/skipDedup 三层去重可以整个删掉 (think.md 砍#4)。
//   - mu + ready 只用来消除"领导权交接瞬间同一任务被挂载两次"的竞态
//     (AddJob 的"建+排" 与 Start 的"全量加载"互斥)。
type Scheduler struct {
	store    store.Store
	parser   *parser.CronParser
	tw       *timewheel.TimeWheel
	interval time.Duration
	slotNum  int

	mu    sync.Mutex
	ready bool
}

// New 创建调度器。interval/slotNum 是时间轮参数 (引擎固定 1s × 60)。
func New(store store.Store, parser *parser.CronParser, interval time.Duration, slotNum int) *Scheduler {
	return &Scheduler{
		store:    store,
		parser:   parser,
		interval: interval,
		slotNum:  slotNum,
	}
}

// Start 上位: 全新时间轮 + 加载全量任务。与 AddJob 的"建+排"互斥,
// 从根上消除交接瞬间重复挂载。
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	s.tw = timewheel.New(s.interval, s.slotNum)
	s.tw.Start()
	s.ready = true
	s.mu.Unlock()

	return s.LoadAndSchedule(ctx)
}

// Stop 让位: 停时间轮。已排队但未到点的任务丢弃, 由新 Leader 重新加载。
// 已在派发途中的 fireOnce 不受影响 (at-least-once 边界)。
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = false
	if s.tw != nil {
		s.tw.Stop()
		s.tw = nil
	}
}

// LoadAndSchedule 引擎重启/故障转移恢复: 把 MySQL 里所有任务重新挂进时间轮。
func (s *Scheduler) LoadAndSchedule(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs, err := s.store.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("从 MySQL 拉取任务失败: %v", err)
	}
	for _, job := range jobs {
		s.scheduleJobLocked(job)
	}
	fmt.Printf("      -> 从 MySQL 成功恢复了 %d 个任务, 已挂载时间轮\n", len(jobs))
	return nil
}

// AddJob 供 API 调用: Leader 收下新任务后, 先落库拿自增 ID, 再挂时间轮。
// 若正处交接期 (ready=false), 只落库, 由随后 Start() 的 LoadAndSchedule 兜底挂载。
func (s *Scheduler) AddJob(ctx context.Context, job *store.JobInfo) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready || s.tw == nil {
		return s.store.CreateJob(ctx, job)
	}
	id, err := s.store.CreateJob(ctx, job)
	if err != nil {
		return 0, err
	}
	s.scheduleJobLocked(job)
	return id, nil
}

// scheduleJobLocked 计算下一触发点、持久化、装进时间轮。调用方必须持 s.mu。
// [砍#1] misfire 补偿已移除: 错过的那一次跳过, 从当前时刻重排, 行为可预期。
func (s *Scheduler) scheduleJobLocked(job *store.JobInfo) {
	now := time.Now()

	// 宕机漏发感知 (仅提示, 不再补偿): 持久化的触发点已在过去, 说明停机期间漏过
	if job.NextTriggerTime > 0 && job.NextTriggerTime < now.Unix()-5 {
		fmt.Printf("[恢复] 任务 %d 原触发点 %s 已错过, 跳过补偿, 从当前时刻重排\n",
			job.ID, time.Unix(job.NextTriggerTime, 0).Format("15:04:05"))
	}

	// A. 算距离下次执行的延迟 (6 位秒级 Cron)
	delay, err := s.parser.NextDelay(job.Cron)
	if err != nil {
		fmt.Printf("[调度异常] 任务 %d 的 Cron 解析失败: %v\n", job.ID, err)
		return
	}

	// B. 持久化下一触发点 (MySQL 有记忆, 断电/故障转移后新 Leader 能恢复)
	job.NextTriggerTime = now.Add(delay).Unix()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.store.SaveJob(ctx, job); err != nil {
			fmt.Printf("[警告] 写回任务 %d 触发点失败: %v\n", job.ID, err)
		}
	}()

	// C. 到点后: 派发一次 + 自我重排。
	//    slot 在此处快照, 派发异步执行 —— 不能到派发时再读 job.NextTriggerTime (已被改写)。
	slot := job.NextTriggerTime
	s.tw.AddTask(delay, strconv.FormatInt(job.ID, 10), func() {
		go s.fireOnce(job, slot) // 异步派发, 别让网络超时拖慢重排
		s.reschedule(job)
	})
	fmt.Printf(" -> 任务 %d 装填完毕, 预计 %s 后引爆\n", job.ID, delay.Round(time.Second))
}

// reschedule 周期任务自我重排 (带锁包装)。
func (s *Scheduler) reschedule(job *store.JobInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tw == nil {
		// 已让位: 不再排班, 等新 Leader 重新加载
		return
	}
	s.scheduleJobLocked(job)
}

// fireOnce 纯粹的派发逻辑: 确定性执行 ID + 先落日志拿 logId + 单目标派发。
func (s *Scheduler) fireOnce(job *store.JobInfo, slot int64) {
	fmt.Printf("\n[%s] ⚡ 任务 %d 触发, 开始派发\n", time.Now().Format("15:04:05"), job.ID)

	// 确定性执行 ID = jobID:slot, Java 端按此原子去重 (at-least-once 投递的兜底伞)
	execID := fmt.Sprintf("%d:%d", job.ID, slot)
	execParam, _ := json.Marshal(map[string]string{"executionId": execID})

	aliveNodes := registry.GetAliveNodes(job.AppName)
	if len(aliveNodes) == 0 {
		fmt.Printf("   -> 警告: 业务组 [%s] 下没有活着的 Java 机器, 任务跳过\n", job.AppName)
		return
	}
	target, err := router.PickOne(aliveNodes)
	if err != nil {
		fmt.Printf("   -> 路由失败: %v\n", err)
		return
	}

	// 触发前先落一行"运行中"日志拿 logId —— Java 回调按它回填结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logID, err := s.store.CreateLog(ctx, &store.JobLog{
		JobID:           job.ID,
		AppName:         job.AppName,
		ExecutorHandler: job.ExecutorHandler,
		ExecID:          execID,
		TriggerTime:     slot,
		TriggerIP:       target,
	})
	if err != nil {
		fmt.Printf("   -> 写运行日志失败: %v\n", err)
	}

	req := &xxljob.RunReq{
		JobID:           int(job.ID),
		ExecutorHandler: job.ExecutorHandler,
		GlueType:        "BEAN",
		BroadcastIndex:  0,
		BroadcastTotal:  1,
		ExecutorParams:  string(execParam),
		LogID:           logID,
		LogDateTime:     time.Now().UnixMilli(),
	}
	if err := xxljob.Trigger(target, req); err != nil {
		fmt.Printf("   -> 派发失败 (%s): %v\n", target, err)
		// 派发即失败 (执行器不可达) → 不会有回调了, 直接把日志标记为失败
		_ = s.store.UpdateLog(ctx, logID, 500, "派发失败: "+err.Error())
		return
	}
	fmt.Printf("   -> 🚀 成功击中目标 %s, 执行ID=%s, logId=%d\n", target, execID, logID)
}
