package store

import (
	"context"
	"testing"
	"time"
)

func TestEtcdStore(t *testing.T) {
	// 1. 尝试连接本地启动的 etcd 服务端
	endpoints := []string{"127.0.0.1:2379"}
	t.Logf("正在尝试连接本地 etcd: %v", endpoints)
	
	store, err := NewEtcdStore(endpoints)
	if err != nil {
		t.Fatalf("连接 etcd 失败，请确认 etcd.exe 是否已经在运行？错误: %v", err)
	}
	defer store.client.Close()

	// 设置一个 5 秒的上下文超时时间，防止测试卡死
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. 构造一个准备写进真实数据库的任务对象
	job := &JobInfo{
		ID:              "test-job-001",
		Cron:            "0/10 * * * * ?",
		ExecutorHandler: "loanCreditJob",
		AppName:         "loan-service",
		Strategy:        "SHARDING",
	}

	// 3. 测试：【新增/修改】
	t.Log(">>> 正在向 etcd 物理硬盘写入测试任务...")
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	t.Log("写入成功！")

	// 4. 测试：【精确查询】
	t.Log(">>> 正在从 etcd 读取刚才写入的任务...")
	loadedJob, err := store.GetJob(ctx, "test-job-001")
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if loadedJob.ExecutorHandler != "loanCreditJob" {
		t.Fatalf("读取出的数据被篡改了，期望 loanCreditJob，实际: %s", loadedJob.ExecutorHandler)
	}
	t.Logf("读取成功！JSON 被完美反序列化为结构体: %+v", loadedJob)

	// 5. 测试：【前缀扫描】(这是引擎宕机重启时，恢复整个时间轮状态的杀手锏)
	t.Log(">>> 正在利用 WithPrefix 捞取全部任务...")
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		t.Fatalf("全量拉取失败: %v", err)
	}
	t.Logf("全量拉取成功！当前 etcd 内共有 %d 个 NanoJob 任务", len(jobs))

	// 6. 测试：【删除】
	t.Log(">>> 正在从 etcd 清理测试任务...")
	if err := store.DeleteJob(ctx, "test-job-001"); err != nil {
		t.Fatalf("删除失败: %v", err)
	}
	t.Log("测试数据清理完毕，整套强一致性存储链路打通！")
}
