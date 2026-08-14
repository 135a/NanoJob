package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestMySQLStore 需要本地 MySQL (默认 root:123456@tcp(127.0.0.1:3306)/nanojob)。
// 没起 MySQL 时自动 Skip, 不影响 go test ./... 通过。
func TestMySQLStore(t *testing.T) {
	dsn := os.Getenv("NANOJOB_TEST_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4&parseTime=true"
	}
	st, err := NewMySQLStore(dsn)
	if err != nil {
		t.Skipf("跳过: 本地 MySQL 不可用 (%v)", err)
	}
	defer st.db.Close()
	if err := st.EnsureTables(context.Background()); err != nil {
		t.Fatalf("建表失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. 新增 (MySQL 自增 ID)
	job := &JobInfo{Cron: "0/10 * * * * ?", ExecutorHandler: "loanCreditJob", AppName: "loan-service"}
	id, err := st.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("新增失败: %v", err)
	}
	defer st.DeleteJob(ctx, id)
	if id == 0 {
		t.Fatal("自增 ID 不应为 0")
	}

	// 2. 精确查询
	got, err := st.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if got.ExecutorHandler != "loanCreditJob" {
		t.Fatalf("读取数据被篡改: %+v", got)
	}

	// 3. 更新 (写回触发时间)
	got.NextTriggerTime = 1234567890
	if err := st.SaveJob(ctx, got); err != nil {
		t.Fatalf("更新失败: %v", err)
	}

	// 4. 全量列表
	jobs, err := st.ListJobs(ctx)
	if err != nil {
		t.Fatalf("列表失败: %v", err)
	}
	t.Logf("当前任务数: %d", len(jobs))

	// 5. 日志闭环: 触发前插"运行中" → 回调回填
	logID, err := st.CreateLog(ctx, &JobLog{
		JobID: id, AppName: "loan-service", ExecID: "123:456",
		TriggerTime: 1700000000, TriggerIP: "192.168.1.100:9999",
	})
	if err != nil {
		t.Fatalf("插日志失败: %v", err)
	}
	if err := st.UpdateLog(ctx, logID, 200, "success"); err != nil {
		t.Fatalf("回调回填失败: %v", err)
	}
	logs, err := st.ListLogs(ctx, id)
	if err != nil || len(logs) == 0 {
		t.Fatalf("日志查询失败: %v", err)
	}
	if logs[0].HandleCode != 200 || logs[0].HandleMsg != "success" {
		t.Fatalf("回调未生效: %+v", logs[0])
	}
}
