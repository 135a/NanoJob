package main

import (
	"context"
	"fmt"
	"time"

	"nanojob/core/store"
)

func main() {
	// 连接本地 etcd
	etcdStore, err := store.NewEtcdStore([]string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}

	// 构造一条极简跑批任务 (每 10 秒执行一次)
	job := &store.JobInfo{
		ID:              "loan-job-999",
		Cron:            "0/10 * * * * ?",
		ExecutorHandler: "loanInterestJobHandler", // 必须和 Java 里的 @XxlJob 名字一模一样
		AppName:         "loan-service",
		Strategy:        "SHARDING",
	}

	// 强制写入数据库
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	err = etcdStore.SaveJob(ctx, job)
	if err != nil {
		fmt.Printf("写入失败: %v\n", err)
	} else {
		fmt.Println("🎉 种子任务注入成功！")
		fmt.Println("请现在去重启你的 main.go 引擎！")
	}
}
