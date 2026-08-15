package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"nanojob/core/parser"
	"nanojob/core/store"
)

// mockStore 内存版 Store: 记录每次调用的参数, 供断言"落库/写回触发点/建日志"发生了没有。
// 全部方法加锁: SaveJob / fireOnce 都跑在时间轮或异步 goroutine 里, 配合 -race 验证并发安全。
type mockStore struct {
	mu          sync.Mutex
	nextID      int64
	createdJobs []*store.JobInfo
	savedJobs   []*store.JobInfo
	createLogs  []*store.JobLog
	updateLogs  []logUpdate
	listJobs    []*store.JobInfo
}

type logUpdate struct {
	logID      int64
	handleCode int
	handleMsg  string
}

func newMockStore() *mockStore { return &mockStore{nextID: 1} }

func (m *mockStore) CreateJob(_ context.Context, job *store.JobInfo) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job.ID = m.nextID // 模拟 MySQL 自增写回 job.ID
	m.nextID++
	m.createdJobs = append(m.createdJobs, job)
	return job.ID, nil
}

func (m *mockStore) SaveJob(_ context.Context, job *store.JobInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.savedJobs = append(m.savedJobs, job)
	return nil
}

func (m *mockStore) GetJob(_ context.Context, _ int64) (*store.JobInfo, error) { return nil, nil }

func (m *mockStore) ListJobs(_ context.Context) ([]*store.JobInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listJobs, nil
}

func (m *mockStore) DeleteJob(_ context.Context, _ int64) error { return nil }

func (m *mockStore) CreateLog(_ context.Context, log *store.JobLog) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createLogs = append(m.createLogs, log)
	return int64(len(m.createLogs)), nil
}

func (m *mockStore) UpdateLog(_ context.Context, logID int64, handleCode int, handleMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateLogs = append(m.updateLogs, logUpdate{logID, handleCode, handleMsg})
	return nil
}

func (m *mockStore) ListLogs(_ context.Context, _ int64) ([]*store.JobLog, error) { return nil, nil }

// ---- 计数辅助 (mockStore.mu 由各方法持有, 读取也走锁) ----

func (m *mockStore) createdCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.createdJobs)
}

func (m *mockStore) savedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.savedJobs)
}

func (m *mockStore) logCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.createLogs)
}

// waitFor 轮询等待条件成立, 超时失败 (调度器的 SaveJob / 触发都在 goroutine 里, 需等异步落定)。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时 (%v)", timeout)
}

// ---- 交接期/就绪期: AddJob 的"建+排" ----

// TestAddJobWhenNotReadyOnlyPersists 交接期 (ready=false): 新任务只落库, 不得排期。
// 这是 Start 的全量加载与 AddJob 的建+排互斥的关键保护 —— 挂轮子的事交给随后 Start 兜底。
func TestAddJobWhenNotReadyOnlyPersists(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10) // 从未 Start → ready=false

	job := &store.JobInfo{Cron: "0 0 * * * *", ExecutorHandler: "demoJobHandler", AppName: "demo"}
	id, err := s.AddJob(context.Background(), job)
	if err != nil {
		t.Fatalf("AddJob 报错: %v", err)
	}
	if id == 0 {
		t.Fatal("CreateJob 应返回自增 ID")
	}
	if ms.createdCount() != 1 {
		t.Fatalf("应恰好落库一次, 实际 %d", ms.createdCount())
	}
	if ms.savedCount() != 0 {
		t.Fatalf("交接期只落库: 不应写回触发点, 实际写回 %d 次", ms.savedCount())
	}
}

// TestAddJobWhenReadyPersistsAndSchedules 就绪期: 落库 + 解析 Cron + 持久化未来触发点 + 挂轮子。
// 用"每小时"的慢 Cron 保证测试期间不触发, 只验证"排上了"这一步。
func TestAddJobWhenReadyPersistsAndSchedules(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}
	defer s.Stop()

	job := &store.JobInfo{Cron: "0 0 * * * *", ExecutorHandler: "demoJobHandler", AppName: "demo"}
	if _, err := s.AddJob(context.Background(), job); err != nil {
		t.Fatalf("AddJob 报错: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return ms.savedCount() >= 1 }) // 异步写回触发点
	if !(job.NextTriggerTime > time.Now().Unix()) {
		t.Fatalf("应持久化未来触发点, NextTriggerTime=%d", job.NextTriggerTime)
	}
	// 慢 Cron 不会立刻触发 → 不应有执行日志
	if ms.logCount() != 0 {
		t.Fatalf("未到点不应产生执行日志, 实际 %d", ms.logCount())
	}
}

// ---- 故障转移恢复 ----

// TestStartLoadsExistingJobs Start 时把 MySQL 里已有的任务全量挂回时间轮 (LoadAndSchedule)。
func TestStartLoadsExistingJobs(t *testing.T) {
	ms := newMockStore()
	ms.listJobs = []*store.JobInfo{
		{ID: 1, Cron: "0 0 * * * *", ExecutorHandler: "h1", AppName: "app"},
		{ID: 2, Cron: "0 0 * * * *", ExecutorHandler: "h2", AppName: "app"},
	}
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}
	defer s.Stop()

	// 两个任务都应被排期 (各自异步写回触发点)
	waitFor(t, 2*time.Second, func() bool { return ms.savedCount() == 2 })
	for _, j := range ms.listJobs {
		if j.NextTriggerTime == 0 {
			t.Fatalf("任务 %d 未被排期 (未写触发点)", j.ID)
		}
	}
}

// ---- 让位 ----

// TestStopThenAddOnlyPersists Stop 后 ready=false: 再来的新任务只落库, 由新 Leader 加载。
func TestStopThenAddOnlyPersists(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}
	s.Stop() // 让位

	job := &store.JobInfo{Cron: "0 0 * * * *", ExecutorHandler: "demoJobHandler", AppName: "demo"}
	if _, err := s.AddJob(context.Background(), job); err != nil {
		t.Fatalf("AddJob 报错: %v", err)
	}
	if ms.createdCount() != 1 {
		t.Fatalf("应落库一次, 实际 %d", ms.createdCount())
	}
	if ms.savedCount() != 0 {
		t.Fatalf("让位后只落库: 不应写回触发点, 实际 %d", ms.savedCount())
	}
}

// ---- 完整闭环: mount → fire → reschedule ----

// TestFireReschedules 端到端单测: 每秒 Cron 挂进轮子 → 到点触发 → reschedule 写回下一次触发点。
// fireOnce 因注册表无存活节点走"跳过"分支 (registry 未初始化), 不碰 Redis/HTTP, 纯内存闭环。
func TestFireReschedules(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 50*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}
	defer s.Stop()

	job := &store.JobInfo{Cron: "* * * * * *", ExecutorHandler: "demoJobHandler", AppName: "demo"} // 每秒触发
	if _, err := s.AddJob(context.Background(), job); err != nil {
		t.Fatalf("AddJob 报错: %v", err)
	}

	// 首次挂载写 1 次, 触发后 reschedule 再写 → ≥2 次才说明"触发了并重新排班了"
	waitFor(t, 4*time.Second, func() bool { return ms.savedCount() >= 2 })
	// fireOnce 无存活节点 → 不应产生执行日志 (走了"跳过"分支而非崩溃)
	if ms.logCount() != 0 {
		t.Fatalf("无存活节点不应产生执行日志, 实际 %d", ms.logCount())
	}
}

// TestStopPreventsReschedule 让位后已到点的回调不得再排班 (tw==nil → reschedule 直接返回)。
func TestStopPreventsReschedule(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}

	job := &store.JobInfo{Cron: "* * * * * *", ExecutorHandler: "demoJobHandler", AppName: "demo"}
	if _, err := s.AddJob(context.Background(), job); err != nil {
		t.Fatalf("AddJob 报错: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return ms.savedCount() >= 1 }) // 已挂载

	s.Stop() // 让位, 之后任何到点的回调都不得再排班
	before := ms.savedCount()
	time.Sleep(2500 * time.Millisecond) // 跨过 ≥2 个触发点
	if ms.savedCount() != before {
		t.Fatalf("Stop 后不应再重排: 触发点写回 %d → %d 次", before, ms.savedCount())
	}
}

// ---- 异常输入 ----

// TestInvalidCronDoesNotSchedule 非法 Cron: 任务照常落库 (由用户自查配置), 但不写触发点、不挂轮子。
func TestInvalidCronDoesNotSchedule(t *testing.T) {
	ms := newMockStore()
	s := New(ms, parser.NewCronParser(), 20*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start 报错: %v", err)
	}
	defer s.Stop()

	job := &store.JobInfo{Cron: "not-a-cron", ExecutorHandler: "demoJobHandler", AppName: "demo"}
	if _, err := s.AddJob(context.Background(), job); err != nil {
		t.Fatalf("AddJob 不应报错 (任务已落库): %v", err)
	}
	if ms.savedCount() != 0 {
		t.Fatalf("非法 Cron 不应写回触发点, 实际 %d 次", ms.savedCount())
	}
}
