package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"nanojob/core/store"
)

// seed 向 MySQL 注入一条演示任务 (每 10 秒触发一次), 供快速验证调度链路。
func main() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4&parseTime=true"
	if v := os.Getenv("NANOJOB_DSN"); v != "" {
		dsn = v
	}

	st, err := store.NewMySQLStore(dsn)
	if err != nil {
		panic(err)
	}

	job := &store.JobInfo{
		Cron:            "0/10 * * * * ?",
		ExecutorHandler: "loanInterestJobHandler", // 必须和 Java 里的 @XxlJob 名字一模一样
		AppName:         "loan-service",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	id, err := st.CreateJob(ctx, job)
	if err != nil {
		fmt.Printf("写入失败: %v\n", err)
	} else {
		fmt.Printf("🎉 种子任务注入成功! ID=%d, 每 10 秒触发一次\n", id)
		fmt.Println("请启动 nanojob 引擎 (go run ./cmd/nanojob -c conf.json), Leader 会自动接管调度")
	}
}
