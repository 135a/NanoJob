package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"nanojob/adapter/xxljob"
	"nanojob/api"
	"nanojob/core/parser"
	"nanojob/core/registry"
	"nanojob/core/router"
	"nanojob/core/store"
	"nanojob/core/timewheel"
	"nanojob/pkg/uid"
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

	// 1.5 从 etcd 动态抢占 WorkerID (大厂机房级解决方案)
	workerID, err := store.ClaimWorkerID(etcdStore.GetClient(), "nanojob-engine")
	if err != nil {
		panic(fmt.Sprintf("致命错误：无法从 etcd 分配机器工号！ %v", err))
	}
	if err := uid.Init(workerID); err != nil {
		panic(fmt.Sprintf("致命错误：雪花算法初始化失败！ %v", err))
	}

	// 2. 初始化 Cron 解析器
	cronParser = parser.NewCronParser()
	fmt.Println("[2/5] Cron 翻译官已就位，支持 Spring 6位秒级语法！")

	// 3. 初始化无状态注册表 (注入 etcd 客户端)
	registry.Init(etcdStore.GetClient())
	fmt.Println("[3/5] 基于 etcd Lease 的无状态 Registry 启动成功！")

	// 4. 初始化内存时间轮 (先初始化防止 API 调用报空指针)
	tw = timewheel.New(1*time.Second, 60)

	// 5. [架构重构] 控制面脑裂与分布式选主 (Control Plane Split-Brain & Leader Election)
	// ⚠️ 必须把竞选逻辑放入后台协程，绝对不能阻塞主线程启动 HTTP Server，否则 Standby 节点将无法接收心跳！
	go func() {
		fmt.Println("\n[4/5] 🛡️ 正在进行全局 Leader 竞选，后台阻塞等待上位...")
		
		// 创建 5秒 租约 (TTL=5)
		// 这里的 concurrency.WithTTL(5) 是一个“函数式选项 (Functional Option)”。
		// 我们可以在后面继续追加其它可选配置，例如：
		// concurrency.WithContext(ctx)      - 绑定特定的上下文，用于提前取消会话
		// concurrency.WithSessionID(id)     - 指定一个已存在的 Lease ID 来恢复会话
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

		// 引擎重启恢复！从 etcd 捞出所有存量任务 + 记录全局 Revision (仅 Leader 运行)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		jobs, rev, err := etcdStore.ListJobs(ctx)
		if err != nil {
			fmt.Printf("警告：从 etcd 拉取任务失败: %v\n", err)
		} else {
			fmt.Printf("      -> 从 etcd 成功恢复了 %d 个历史任务，开始挂载...\n", len(jobs))
			for _, job := range jobs {
				scheduleJob(job, false)
			}
		}

		// =========================================================
		// ⭐️ [架构修复 1] Watch 统一消费：任意引擎写入 etcd，Leader 通过 Watch 感知增量
		//    彻底解决"任务打到 Standby 变孤儿"：调度权收拢到 Leader，Standby 只写不调度。
		//    read-then-watch：从 rev+1 开始监听，封死"List 与 Watch 之间新增任务被漏掉"的竞态窗口。
		//    收到事件就 scheduleJob；scheduleJob 内部按 (jobID, 触发点) 去重，
		//    防住"Leader 自己写回 NextTriggerTime 的 Put 被自己 Watch 到 → 自旋重复挂载"。
		// =========================================================
		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		defer watcherCancel()
		go func() {
			for wresp := range etcdStore.WatchJobs(watcherCtx, rev+1) {
				for _, ev := range wresp.Events {
					// 跳过删除事件：已删除的任务不该被重新挂载
					// (防止 ListJobs 失败、rev=0 兜底全量回放时，把历史删除的任务"复活")
					if ev.Type == clientv3.EventTypeDelete {
						continue
					}
					var job store.JobInfo
					if err := json.Unmarshal(ev.Kv.Value, &job); err == nil {
						scheduleJob(&job, false)
					}
				}
			}
		}()

		// =========================================================
		// ⭐️ [架构修复 2] fail-fast 夺权监控：租约一丢，立即停轮子、停 Watch、退出
		//    替换原来的 select{}。select{} 永不检查租约，旧 Leader 失联后仍僵尸化派发 = 脑裂。
		//    这里监听两个互补信号：
		//      - session.Done()    = 本地信号（etcd 连接断开 / 租约被撤销），etcd 不可达时依然有效
		//      - election.Observe() = 远端信号（leader key 被删 / 被替换，可能本机租约还活着）
		//    一旦触发：tw.Stop() 停时间轮 + watcherCancel() 停 Watch + return。
		//    return 退出后，defer session.Close() 撤销租约 → Standby 干净接管，脑裂闭环。
		// =========================================================
		observeCh := election.Observe(context.Background())
		for {
			select {
			case <-session.Done():
				fmt.Printf("\n⚠️ [%s] 租约丢失或 etcd 连接断开！本节点不再是 Leader，停止调度...\n", nodeID)
				tw.Stop()
				watcherCancel()
				return
			case resp := <-observeCh:
				// 远端信号：leader key 每次变化都会推一条 GetResponse。
				// ⚠️ Observe 可能先推一条"当前主还是我自己"，此时绝不能退出——
				// 一退出就撤销租约、让位，会变成"当选即让位"的活锁。
				// 只有确认"key 被删 / 主已被别人顶替"，才停止调度。
				// ⚠️ 断连时库内部 Get 失败会 close(observeCh)（election.go 的 observe 协程），
				// 从已关闭 channel 收到的是 nil，必须判空，否则 resp.Kvs 空指针 panic。
				// 若真断连，session.Done() 也会触发，两条路径都汇到同一个"停"。
				if resp == nil || len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != nodeID {
					fmt.Printf("\n⚠️ [%s] 已被新 Leader 取代！本节点停止调度...\n", nodeID)
					tw.Stop()
					watcherCancel()
					return
				}
			}
		}
	}()

	// 6. 注册管理后台 API (提供给可视化网页调用)
	// [架构修复 1] 不再传 ScheduleNotify：任务写入 etcd 后由 Leader 的 Watch 统一消费。
	//   任意引擎（含 Standby）都能安全收写入，任务不会因"打到 Standby"而变孤儿。
	jobApi := &api.JobAPI{
		Store: etcdStore,
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

// fireOnce 纯粹的派发逻辑，打完仗就撤，绝不自循环（供正常触发和 Misfire 补偿复用）
// slot: 本次触发的计划时间戳(Unix秒)，用于构造确定性执行 ID。
// ⚠️ 必须由调用方显式传入，不能在函数内部读 job.NextTriggerTime —— 派发是异步的，
//    内部读可能读到 scheduleJob 已改写好的"下一周期"值，执行 ID 就对不上了。
func fireOnce(job *store.JobInfo, slot int64) {
	fmt.Printf("\n[%s] ⚡ 任务触发！开始派发 -> %s\n", time.Now().Format("15:04:05"), job.ID)

	// ⭐️ [架构修复 3] 确定性执行 ID = 任务ID + 触发时间戳 (例: 1834567890123456789:1723456789)
	//    目的：让"旧主合法派发 slot N" 和 "新主 misfire 补偿 slot N" 算出同一个 ID，
	//    Java 执行器按此 ID 原子去重，重复派发直接跳过 —— at-least-once 投递的兜底伞。
	//    ⚠️ 必须确定性派生，绝不能用随机 UUID：随机 ID 两次派发不一样，执行器认不出重复。
	execID := job.ID + ":" + strconv.FormatInt(slot, 10)
	execParam, _ := json.Marshal(map[string]string{"executionId": execID})

	aliveNodes := registry.GetAliveNodes(job.AppName)
	if len(aliveNodes) == 0 {
		fmt.Printf("   -> 警告：业务组 [%s] 下没有活着的 Java 机器，任务只能跳过。\n", job.AppName)
		return
	}
	
	shardResults, _ := router.Route(router.StrategySharding, aliveNodes)

	for _, shard := range shardResults {
		go func(s router.ShardResult) {
			// 把字符串类型的 job.ID 转成 XXL-Job 客户端要求的数字类型
			realJobID, err := strconv.Atoi(job.ID)
			if err != nil {
				// 兜底处理：如果解析失败默认传 0，避免程序崩溃。
				fmt.Printf("   -> ⚠️ 警告：任务 ID [%s] 无法转换为数字类型, %v\n", job.ID, err)
			}

			req := &xxljob.RunReq{
				JobID:           realJobID,
				ExecutorHandler: job.ExecutorHandler,
				GlueType:        "BEAN",
				BroadcastIndex:  s.BroadcastIndex,
				BroadcastTotal:  s.BroadcastTotal,
				// executionId 封装在 executorParams 里透传给 Java 执行器：
				// xxl-job 的 TriggerParam 没有 executionId 字段，executorParams 是唯一透传通道，
				// Java 端用 XxlJobHelper.getJobParam() 拿回来解析。
				ExecutorParams: string(execParam),
			}
			if err := xxljob.Trigger(s.TargetIP, req); err != nil {
				fmt.Printf("   -> 派发失败 (%s): %v\n", s.TargetIP, err)
			} else {
				fmt.Printf("   -> 🚀 成功击中目标 %s (分片 %d/%d), 执行ID=%s\n", s.TargetIP, s.BroadcastIndex, s.BroadcastTotal, execID)
			}
		}(shard)
	}
}

var (
	// [架构修复 1 配套] 时间轮挂载去重表
	//   jobID -> 已挂载的"未来触发点"(Unix秒字符串)。
	//   Leader 通过 Watch 消费增量后，scheduleJob 会把算好的 NextTriggerTime 写回 etcd，
	//   而 Leader 的 Watch 会收到自己写回的 Put → 再次进入 scheduleJob。
	//   用 (jobID, 触发点) 判重：同一未来触发点已在轮子里就忽略，防止自旋重复挂载。
	//   ⚠️ 不能只按 jobID 判重 —— 周期性任务每轮都会合法重排出新的触发点。
	//   lastFired 额外挡住"触发点已到点消费、但写回事件延迟到达"的陈旧 Watch 事件。
	wheelMu   sync.Mutex
	inWheel   = make(map[string]string) // jobID -> 已挂载的未来触发点
	lastFired = make(map[string]string) // jobID -> 最近一次已派发/已消费的触发点
)

// scheduleJob 核心魔法：算时间、塞入轮子、自动循环
// skipDedup: 仅 triggerFunc 里的"合法下一周期重排"传 true。
//   此时 job 刚从轮子到点弹出，incoming 必然等于刚派发的 slot，
//   若也走判重，会被 lastFired 误伤 → 周期性任务跑一次就死。跳过判重即可安全重排。
func scheduleJob(job *store.JobInfo, skipDedup bool) {
	now := time.Now().Unix()

	// ⭐️ [架构修复 1 配套] 挂载去重
	//    先取"进入调度时的触发点"，再决定是否挂载。
	//    ⚠️ 必须在改写 NextTriggerTime 之前判重！否则 incoming 会读到新值，永远匹配不上。
	key := job.ID
	incoming := job.NextTriggerTime
	incomingStr := strconv.FormatInt(incoming, 10)
	if !skipDedup {
		wheelMu.Lock()
		if prev, ok := inWheel[key]; ok && prev == incomingStr {
			wheelMu.Unlock()
			fmt.Printf("   -> 任务 %s 触发点 %d 已在轮子中，忽略重复挂载\n", key, incoming)
			return
		}
		// 该触发点已经被"到点派发"消费过，只是写回 etcd 的事件延迟到达 (陈旧事件)，忽略
		if prev, ok := lastFired[key]; ok && prev == incomingStr {
			wheelMu.Unlock()
			fmt.Printf("   -> 任务 %s 触发点 %d 已派发过，忽略迟到的 Watch 事件\n", key, incoming)
			return
		}
		wheelMu.Unlock()
	}

	// ⭐️ Misfire 漏发补偿机制 ⭐️
	// 如果配置里存在预期的执行时间，而且当前时间已经超过了预期时间 (给 5 秒网络宽限期),因为延迟是常见的,不应把任何延迟视为漏发,所以我们给了一个5秒的宽限期,如果超过5秒就认为是漏发了
	if job.NextTriggerTime > 0 {
		if job.NextTriggerTime < now-5 {
			fmt.Printf("\n[Misfire 预警] 发现任务 %s 在宕机期间漏发！立即触发 [FIRE_ONCE_NOW] 补偿机制！\n", job.ID)
			
			// ⭐️ [架构修复 3] 先同步快照"漏掉的那一次 slot"，再异步派发。
			//    不能直接 go fireOnce(job, job.NextTriggerTime)：go 是异步的，
			//    派发真正执行时 job.NextTriggerTime 可能已被下面的 scheduleJob 改写为下一周期值，
			//    执行 ID 就对不上"旧主已派发"的 ID，Java 端去重失效。
			missedSlot := job.NextTriggerTime
			go fireOnce(job, missedSlot)
		} else if job.NextTriggerTime <= now {
			// TODO: 轻微迟到 (0~5秒内) 或刚好到期。目前代码会直接跳过当次执行，将其安排在下个周期。
			// 按照大厂标准，这里应该和”没有延迟”一样，立刻触发当次执行，然后再算下一次的时间扔进时间轮。
			// go fireOnce(job, job.NextTriggerTime)
		}
	}

	// A. 翻译官出马：算一下距离下一次执行还有多少秒
	delay, err := cronParser.NextDelay(job.Cron)
	if err != nil {
		fmt.Printf("[调度异常] 任务 %s 的 Cron 解析失败: %v\n", job.ID, err)
		return
	}

	// ⭐️ 持久化记忆 ⭐️
	// 算出下一次真实的绝对时间戳，并异步写回 etcd！这样哪怕下一秒断电，系统也有记忆！
	job.NextTriggerTime = time.Now().Add(delay).Unix()
	go etcdStore.SaveJob(context.Background(), job)

	// 记录本轮已挂载的触发点（供上面判重用）
	wheelMu.Lock()
	inWheel[key] = strconv.FormatInt(job.NextTriggerTime, 10)
	wheelMu.Unlock()

	// B. 定义这个任务“到点后真正要干的活”
	var triggerFunc func()
	triggerFunc = func() {
		// 0. 当前触发点已被消费：清挂载标记 + 记录已派发点。
		//    清 inWheel 才能让下一周期合法重排；记 lastFired 让迟到的 Watch 陈旧事件被挡掉。
		wheelMu.Lock()
		delete(inWheel, key)
		lastFired[key] = strconv.FormatInt(job.NextTriggerTime, 10)
		wheelMu.Unlock()

		// 1. 打仗 (同步取"当前引爆点"作为执行 ID 的一部分)
		fireOnce(job, job.NextTriggerTime)
		// 2. 灵魂自循环：重新排队！(skipDedup=true：这是合法的下一周期重排，不走判重)
		scheduleJob(job, true)
	}

	// C. 正式把这个闭包函数，扔进时间轮排队
	tw.AddTask(delay, job.ID, triggerFunc)
	fmt.Printf(" -> 任务装填完毕: %s, 预计 %d 秒后引爆\n", job.ID, int(delay.Seconds()))
}
