package api

import (
	"context"
	"sync"

	"nanojob/core/store"
)

// mockStore 内存版 Store, 供 api handler 测试断言"调用了哪些持久化方法、参数是什么"。
// handler 在并发/异步路径下跑, 全部方法加锁, 配合 -race 验证。
type mockStore struct {
	mu        sync.Mutex
	jobs      []*store.JobInfo // ListJobs 返回
	logs      []*store.JobLog  // ListLogs 返回
	created   []*store.JobInfo // CreateJob 收到的
	saved     []*store.JobInfo // SaveJob 收到的
	updates   []updateCall     // UpdateLog 收到的
	nextID    int64
	listErr   error
	updateErr error
}

type updateCall struct {
	logID int64
	code  int
	msg   string
}

func newMockStore() *mockStore { return &mockStore{nextID: 1} }

func (m *mockStore) CreateJob(_ context.Context, job *store.JobInfo) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job.ID = m.nextID // 模拟 MySQL 自增写回
	m.nextID++
	m.created = append(m.created, job)
	return job.ID, nil
}

func (m *mockStore) SaveJob(_ context.Context, job *store.JobInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saved = append(m.saved, job)
	return nil
}

func (m *mockStore) GetJob(_ context.Context, _ int64) (*store.JobInfo, error) { return nil, nil }

func (m *mockStore) ListJobs(_ context.Context) ([]*store.JobInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs, m.listErr
}

func (m *mockStore) DeleteJob(_ context.Context, _ int64) error { return nil }

func (m *mockStore) CreateLog(_ context.Context, _ *store.JobLog) (int64, error) { return 0, nil }

func (m *mockStore) UpdateLog(_ context.Context, logID int64, code int, msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, updateCall{logID, code, msg})
	return m.updateErr
}

func (m *mockStore) ListLogs(_ context.Context, _ int64) ([]*store.JobLog, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logs, nil
}

// createdCount 读已落库的任务数。
func (m *mockStore) createdCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.created)
}
