package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"nanojob/api"
	"nanojob/core/election"
	"nanojob/core/parser"
	"nanojob/core/registry"
	"nanojob/core/scheduler"
	"nanojob/core/store"
	"nanojob/pkg/config"
)

func main() {
	configPath := flag.String("c", "conf.json", "配置文件路径")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println("🚀 NanoJob 分布式调度引擎启动中...")
	fmt.Println("========================================")

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(fmt.Sprintf("致命错误: %v", err))
	}

	// 1. MySQL 持久层 (任务配置 + 触发时间 + 执行日志)
	mysqlStore, err := store.NewMySQLStore(cfg.MySQL.DSN)
	if err != nil {
		panic(fmt.Sprintf("致命错误: %v", err))
	}
	if err := mysqlStore.EnsureTables(context.Background()); err != nil {
		panic(fmt.Sprintf("致命错误: %v", err))
	}
	fmt.Println("[1/5] MySQL 持久层连接成功, 任务/日志表已就绪")

	// 2. Redis (选举锁 + 执行器注册表)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		panic(fmt.Sprintf("致命错误: 无法连接 Redis [%s]! %v", cfg.Redis.Addr, err))
	}
	registry.Init(redisClient)
	fmt.Println("[2/5] Redis 连接成功 (选主 + 执行器注册表)")

	// 3. Cron 解析器 + 调度器 (内含 1s×60 时间轮)
	cronParser := parser.NewCronParser()
	sched := scheduler.New(mysqlStore, cronParser, 1*time.Second, 60)
	fmt.Println("[3/5] Cron 解析器就绪, 支持 Spring 6 位秒级语法")

	// 4. Redis 选主 (SETNX + TTL 自研锁, 5s 租约)
	//    节点标识 = 本节点对外地址: 既是锁持有值, 也是 Standby 重定向的 Leader 地址
	nodeID := cfg.APIServer.HTTP.AdvertiseAddr
	if nodeID == "" {
		nodeID = fmt.Sprintf("http://127.0.0.1:%s", cfg.APIServer.HTTP.Port)
	}
	elect := election.New(redisClient, "nanojob:election:"+cfg.ClusterName, nodeID, 5*time.Second)

	// 5. 后台选举循环 + 领导权变化驱动 上位(start) / 让位(stop)
	go func() {
		errCh := make(chan error, 4)
		go elect.LoopInElect(context.Background(), errCh)
		go func() {
			for e := range errCh {
				fmt.Printf("[选举异常] %v\n", e)
			}
		}()

		for isLeader := range elect.Changes() {
			if isLeader {
				fmt.Printf("🔥 [%s] 接管调度, 启动时间轮并恢复任务...\n", nodeID)
				if err := sched.Start(context.Background()); err != nil {
					fmt.Printf("[严重] Leader 启动失败: %v\n", err)
				}
			} else {
				fmt.Printf("⚠️ [%s] 已让位, 停止时间轮\n", nodeID)
				sched.Stop()
			}
		}
	}()

	// 6. 管理 API (写收敛 Leader) + 回调闭环 + 执行器心跳
	jobAPI := &api.JobAPI{Store: mysqlStore, Scheduler: sched, Election: elect}
	jobAPI.RegisterRoutes()

	// 回调端点所有节点都注册: 日志落共享 MySQL, 不必收敛 Leader
	callbackAPI := &api.CallbackAPI{Store: mysqlStore}
	http.HandleFunc("/api/callback", callbackAPI.Handle)

	http.HandleFunc("/api/registry", registry.ReceiveHeartbeat)

	// 7. 优雅退出 (Ctrl+C / 终止信号)
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		fmt.Println("\n收到退出信号, 正在停止...")
		elect.StopElect()
		sched.Stop()
		os.Exit(0)
	}()

	listenAddr := ":" + cfg.APIServer.HTTP.Port
	if cfg.APIServer.HTTP.Host != "" {
		listenAddr = cfg.APIServer.HTTP.Host + ":" + cfg.APIServer.HTTP.Port
	}
	fmt.Printf("\n✅ NanoJob 启动完成! 监听 %s, 节点对外地址 %s\n", listenAddr, nodeID)
	fmt.Println("   管理界面: ui/index.html | 心跳: /api/registry | 回调: /api/callback")
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		panic(err)
	}
}
