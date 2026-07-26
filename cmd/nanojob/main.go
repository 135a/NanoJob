package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"

	"nanojob/adapter/xxljob"
	"nanojob/api"
	"nanojob/core/parser"
	"nanojob/core/registry"
	"nanojob/core/router"
	"nanojob/core/store"
	"nanojob/core/timewheel"
)

var (
	etcdStore  *store.EtcdStore
	cronParser *parser.CronParser
	tw         *timewheel.TimeWheel
)

func main() {
	// 解析命令行启动参数
	etcdAddr := flag.String("etcd", "127.0.0.1:2379", "etcd 节点地址，多个用逗号分隔 (例: 10.0.0.1:2379,10.0.0.2:2379)")
	port := flag.String("port", "8080", "控制台及心跳接口的 HTTP 监听端口")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println("🚀 NanoJob 企业级分布式调度引擎启动中...")
	fmt.Println("========================================")

	// 1. 初始化持久层 (连接 etcd 集群)
	var err error
	endpoints := strings.Split(*etcdAddr, ",")
	etcdStore, err = store.NewEtcdStore(endpoints)
	if err != nil {
		panic(fmt.Sprintf("致命错误：无法连接 etcd 集群 [%s]！ %v", *etcdAddr, err))
	}
	fmt.Println("[1/5] etcd 云原生配置中心连接成功！")

	// 2. 初始化 Cron 解析器
	cronParser = parser.NewCronParser()
	fmt.Println("[2/5] Cron 翻译官已就位，支持 Spring 6位秒级语法！")

	// 3. 启动后台清道夫 (清理失联机器)
	registry.StartMonitor()
	fmt.Println("[3/5] Registry 心跳清道夫启动成功！")

	// 4. 初始化内存时间轮 (先初始化防止 API 调用报空指针)
	tw = timewheel.New(1*time.Second, 60)

	// 5. [架构重构] 控制面脑裂与分布式选主 (Control Plane Split-Brain & Leader Election)
	// ⚠️ 必须把竞选逻辑放入后台协程，绝对不能阻塞主线程启动 HTTP Server，否则 Standby 节点将无法接收心跳！
	go func() {
		fmt.Println("\n[4/5] 🛡️ 正在进行全局 Leader 竞选，后台阻塞等待上位...")
		
		// 创建 5秒 租约 (TTL=5)
		session, err := concurrency.NewSession(etcdStore.GetClient(), concurrency.WithTTL(5))
		if err != nil {
			fmt.Printf("创建 etcd Session 失败: %v\n", err)
			return
		}
		defer session.Close()

		// 创建名为 "/nanojob/election" 的竞选房间
		election := concurrency.NewElection(session, "/nanojob/election")

		// 获取本机Hostname作为节点标识
		hostname, _ := os.Hostname()
		nodeID := fmt.Sprintf("engine-%s", hostname)

		// 开始抢锁！此方法会 阻塞，直到抢到锁为止。未抢到锁的机器将在此永久待命 (Standby)。
		if err := election.Campaign(context.Background(), nodeID); err != nil {
			fmt.Printf("竞选 Leader 失败退出: %v\n", err)
			return
		}

		// =========================================================
		// ⚠️ 只有成功当选为 Leader 的机器，代码才会继续往下执行！ ⚠️
		// =========================================================
		fmt.Printf("🔥 竞选成功！当前节点 [%s] 已接管整个集群调度大权！\n\n", nodeID)

		// 启动内存时间轮 (仅 Leader 运行)
		tw.Start()
		fmt.Println("[5/5] TimeWheel 核心引擎点火成功，开始静默跳动...")

		// 引擎重启恢复！从 etcd 捞出所有存量任务 (仅 Leader 运行)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		jobs, err := etcdStore.ListJobs(ctx)
		if err != nil {
			fmt.Printf("警告：从 etcd 拉取任务失败: %v\n", err)
		} else {
			fmt.Printf("      -> 从 etcd 成功恢复了 %d 个历史任务，开始挂载...\n", len(jobs))
			for _, job := range jobs {
				scheduleJob(job)
			}
		}

		// ⚠️ 极其关键的一步：阻塞当前协程，绝不能让它退出！
		// 如果协程退出，defer session.Close() 就会被执行，etcd 租约会被撤销。
		// 这会导致其他替补节点立刻抢到锁，从而引发所有节点都变成 Leader 的超级脑裂灾难！
		select {}
	}()

	// 6. 注册管理后台 API (提供给可视化网页调用)
	jobApi := &api.JobAPI{
		Store:          etcdStore,
		// 【灵魂联动】前端调接口新增任务时，触发这个钩子，直接调用下面的 scheduleJob 函数进行热启动！
		ScheduleNotify: scheduleJob, 
	}
	jobApi.RegisterRoutes()

	// 7. 启动 HTTP 监听，迎接 Java 兵团的注册
	// TODO: [架构缺陷 3] 接口缺乏安全鉴权 (API Auth)
	// 当前所有 /api 接口均在公网/内网裸奔，极度危险。应当在此处加入 HTTP Middleware (拦截器) 校验安全 Token。
	
	// TODO: [架构缺陷 4] 缺乏执行结果的闭环回调与日志 (Callback & Logging)
	// 当前引擎发包后(fire-and-forget)无法得知 Java 任务的最终成功/失败状态。
	// 需要新增类似 /api/callback 的接口供 Java 机器打完仗后上报战况，并持久化日志至 MySQL/ES 以供前端页面展示。
	http.HandleFunc("/api/registry", registry.ReceiveHeartbeat)
	
	listenUrl := ":" + *port
	fmt.Printf("\n✅ NanoJob 启动完成！正在监听 %s 端口，等待执行器接入...\n", listenUrl)
	if err := http.ListenAndServe(listenUrl, nil); err != nil {
		panic(err)
	}
}

// scheduleJob 核心魔法：算时间、塞入轮子、自动循环
func scheduleJob(job *store.JobInfo) {
	// A. 翻译官出马：算一下距离下一次执行还有多少秒
	delay, err := cronParser.NextDelay(job.Cron)
	if err != nil {
		fmt.Printf("[调度异常] 任务 %s 的 Cron 解析失败: %v\n", job.ID, err)
		return
	}

	// B. 定义这个任务“到点后真正要干的活”
	var triggerFunc func()
	triggerFunc = func() {
		fmt.Printf("\n[%s] ⚡ 任务触发！开始派发 -> %s\n", time.Now().Format("15:04:05"), job.ID)

		// B1. 去注册中心查人
		aliveNodes := registry.GetAliveNodes(job.AppName)
		if len(aliveNodes) == 0 {
			fmt.Printf("   -> 警告：业务组 [%s] 下没有活着的 Java 机器，任务只能跳过。\n", job.AppName)
		} else {
			// B2. 路由分片
			shardResults, _ := router.Route(router.StrategySharding, aliveNodes)

			// B3. 并发开火射击！
			for _, shard := range shardResults {
				go func(s router.ShardResult) {
					req := &xxljob.RunReq{
						JobID:           10086,
						ExecutorHandler: job.ExecutorHandler,
						GlueType:        "BEAN",
						BroadcastIndex:  s.BroadcastIndex,
						BroadcastTotal:  s.BroadcastTotal,
					}
					if err := xxljob.Trigger(s.TargetIP, req); err != nil {
						fmt.Printf("   -> 派发失败 (%s): %v\n", s.TargetIP, err)
						
						// TODO: [Phase 5 进阶容灾] 增加故障转移 (Failover) 逻辑
						// 如果在发包过程中机器突然宕机导致 Trigger 失败：
						// 1. 需要立刻从 aliveNodes 缓存中剔除当前死掉的 s.TargetIP。
						// 2. 从剩余存活机器中重新挑选一台 Backup Node。
						// 3. 将当前的 req (携带相同分片 Index) 重新发送给备用机器。
						// ⚠️ 警告：实现此功能前，务必保证下游 Java 端的 @XxlJob 业务逻辑实现了绝对的【幂等性】，否则会引发重复扣款灾难。
					} else {
						fmt.Printf("   -> 🚀 成功击中目标 %s (分片 %d/%d)\n", s.TargetIP, s.BroadcastIndex, s.BroadcastTotal)
					}
				}(shard)
			}
		}

		// B4. 灵魂自循环：任务执行完后，再调一次自己，去算下下一次的时间，重新排队！
		scheduleJob(job)
	}

	// C. 正式把这个闭包函数，扔进时间轮排队
	tw.AddTask(delay, job.ID, triggerFunc)
	fmt.Printf(" -> 任务装填完毕: %s, 预计 %d 秒后引爆\n", job.ID, int(delay.Seconds()))
}
