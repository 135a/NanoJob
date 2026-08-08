package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
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
	"nanojob/pkg/uid"
)

var (
	etcdStore  *store.EtcdStore
	cronParser *parser.CronParser
	tw         *timewheel.TimeWheel
)

func main() {
	// 瑙ｆ瀽鍛戒护琛屽惎鍔ㄥ弬鏁?	etcdAddr := flag.String("etcd", "127.0.0.1:2379", "etcd 鑺傜偣鍦板潃锛屽涓敤閫楀彿鍒嗛殧 (渚? 10.0.0.1:2379,10.0.0.2:2379)")
	port := flag.String("port", "8080", "鎺у埗鍙板強蹇冭烦鎺ュ彛鐨?HTTP 鐩戝惉绔彛")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println("馃殌 NanoJob 浼佷笟绾у垎甯冨紡璋冨害寮曟搸鍚姩涓?..")
	fmt.Println("========================================")

	// 1. 鍒濆鍖栨寔涔呭眰 (杩炴帴 etcd 闆嗙兢)
	var err error
	endpoints := strings.Split(*etcdAddr, ",")
	etcdStore, err = store.NewEtcdStore(endpoints)
	if err != nil {
		panic(fmt.Sprintf("鑷村懡閿欒锛氭棤娉曡繛鎺?etcd 闆嗙兢 [%s]锛?%v", *etcdAddr, err))
	}
	fmt.Println("[1/5] etcd 浜戝師鐢熼厤缃腑蹇冭繛鎺ユ垚鍔燂紒")

	// 1.5 浠?etcd 鍔ㄦ€佹姠鍗?WorkerID (澶у巶鏈烘埧绾цВ鍐虫柟妗?
	workerID, err := store.ClaimWorkerID(etcdStore.GetClient(), "nanojob-engine")
	if err != nil {
		panic(fmt.Sprintf("鑷村懡閿欒锛氭棤娉曚粠 etcd 鍒嗛厤鏈哄櫒宸ュ彿锛?%v", err))
	}
	if err := uid.Init(workerID); err != nil {
		panic(fmt.Sprintf("鑷村懡閿欒锛氶洩鑺辩畻娉曞垵濮嬪寲澶辫触锛?%v", err))
	}

	// 2. 鍒濆鍖?Cron 瑙ｆ瀽鍣?	cronParser = parser.NewCronParser()
	fmt.Println("[2/5] Cron 缈昏瘧瀹樺凡灏变綅锛屾敮鎸?Spring 6浣嶇绾ц娉曪紒")

	// 3. 鍒濆鍖栨棤鐘舵€佹敞鍐岃〃 (娉ㄥ叆 etcd 瀹㈡埛绔?
	registry.Init(etcdStore.GetClient())
	fmt.Println("[3/5] 鍩轰簬 etcd Lease 鐨勬棤鐘舵€?Registry 鍚姩鎴愬姛锛?)

	// 4. 鍒濆鍖栧唴瀛樻椂闂磋疆 (鍏堝垵濮嬪寲闃叉 API 璋冪敤鎶ョ┖鎸囬拡)
	tw = timewheel.New(1*time.Second, 60)

	// 5. [鏋舵瀯閲嶆瀯] 鎺у埗闈㈣剳瑁備笌鍒嗗竷寮忛€変富 (Control Plane Split-Brain & Leader Election)
	// 鈿狅笍 蹇呴』鎶婄珵閫夐€昏緫鏀惧叆鍚庡彴鍗忕▼锛岀粷瀵逛笉鑳介樆濉炰富绾跨▼鍚姩 HTTP Server锛屽惁鍒?Standby 鑺傜偣灏嗘棤娉曟帴鏀跺績璺筹紒
	go func() {
		fmt.Println("\n[4/5] 馃洝锔?姝ｅ湪杩涜鍏ㄥ眬 Leader 绔為€夛紝鍚庡彴闃诲绛夊緟涓婁綅...")
		
		// 鍒涘缓 5绉?绉熺害 (TTL=5)
		// 杩欓噷鐨?concurrency.WithTTL(5) 鏄竴涓€滃嚱鏁板紡閫夐」 (Functional Option)鈥濄€?		// 鎴戜滑鍙互鍦ㄥ悗闈㈢户缁拷鍔犲叾瀹冨彲閫夐厤缃紝渚嬪锛?		// concurrency.WithContext(ctx)      - 缁戝畾鐗瑰畾鐨勪笂涓嬫枃锛岀敤浜庢彁鍓嶅彇娑堜細璇?		// concurrency.WithSessionID(id)     - 鎸囧畾涓€涓凡瀛樺湪鐨?Lease ID 鏉ユ仮澶嶄細璇?		session, err := concurrency.NewSession(etcdStore.GetClient(), concurrency.WithTTL(5))
		if err != nil {
			fmt.Printf("鍒涘缓 etcd Session 澶辫触: %v\n", err)
			return
		}
		defer session.Close()

		// 鍒涘缓鍚嶄负 "/nanojob/election" 鐨勭珵閫夋埧闂?		election := concurrency.NewElection(session, "/nanojob/election")

		// 鑾峰彇鏈満Hostname浣滀负鑺傜偣鏍囪瘑
		hostname, _ := os.Hostname()
		nodeID := fmt.Sprintf("engine-%s", hostname)

		// 寮€濮嬫姠閿侊紒姝ゆ柟娉曚細 闃诲锛岀洿鍒版姠鍒伴攣涓烘銆傛湭鎶㈠埌閿佺殑鏈哄櫒灏嗗湪姝ゆ案涔呭緟鍛?(Standby)銆?		if err := election.Campaign(context.Background(), nodeID); err != nil {
			fmt.Printf("绔為€?Leader 澶辫触閫€鍑? %v\n", err)
			return
		}

		// =========================================================
		// 鈿狅笍 鍙湁鎴愬姛褰撻€変负 Leader 鐨勬満鍣紝浠ｇ爜鎵嶄細缁х画寰€涓嬫墽琛岋紒 鈿狅笍
		// =========================================================
		fmt.Printf("馃敟 绔為€夋垚鍔燂紒褰撳墠鑺傜偣 [%s] 宸叉帴绠℃暣涓泦缇よ皟搴﹀ぇ鏉冿紒\n\n", nodeID)

		// 鍚姩鍐呭瓨鏃堕棿杞?(浠?Leader 杩愯)
		tw.Start()
		fmt.Println("[5/5] TimeWheel 鏍稿績寮曟搸鐐圭伀鎴愬姛锛屽紑濮嬮潤榛樿烦鍔?..")

		// 寮曟搸閲嶅惎鎭㈠锛佷粠 etcd 鎹炲嚭鎵€鏈夊瓨閲忎换鍔?(浠?Leader 杩愯)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		jobs, err := etcdStore.ListJobs(ctx)
		if err != nil {
			fmt.Printf("璀﹀憡锛氫粠 etcd 鎷夊彇浠诲姟澶辫触: %v\n", err)
		} else {
			fmt.Printf("      -> 浠?etcd 鎴愬姛鎭㈠浜?%d 涓巻鍙蹭换鍔★紝寮€濮嬫寕杞?..\n", len(jobs))
			for _, job := range jobs {
				scheduleJob(job)
			}
		}

		// 鈿狅笍 鏋佸叾鍏抽敭鐨勪竴姝ワ細闃诲褰撳墠鍗忕▼锛岀粷涓嶈兘璁╁畠閫€鍑猴紒
		// 濡傛灉鍗忕▼閫€鍑猴紝defer session.Close() 灏变細琚墽琛岋紝etcd 绉熺害浼氳鎾ら攢銆?		// 杩欎細瀵艰嚧鍏朵粬鏇胯ˉ鑺傜偣绔嬪埢鎶㈠埌閿侊紝浠庤€屽紩鍙戞墍鏈夎妭鐐归兘鍙樻垚 Leader 鐨勮秴绾ц剳瑁傜伨闅撅紒
		select {}
	}()

	// 6. 娉ㄥ唽绠＄悊鍚庡彴 API (鎻愪緵缁欏彲瑙嗗寲缃戦〉璋冪敤)
	jobApi := &api.JobAPI{
		Store:          etcdStore,
		// 銆愮伒榄傝仈鍔ㄣ€戝墠绔皟鎺ュ彛鏂板浠诲姟鏃讹紝瑙﹀彂杩欎釜閽╁瓙锛岀洿鎺ヨ皟鐢ㄤ笅闈㈢殑 scheduleJob 鍑芥暟杩涜鐑惎鍔紒
		ScheduleNotify: scheduleJob, 
	}
	jobApi.RegisterRoutes()

	// 7. 娉ㄥ唽鎵ц缁撴灉鍥炶皟鎺ュ彛 (Issue #5: Callback & Logging)
	callbackApi := &api.CallbackAPI{EtcdClient: etcdStore.GetClient()}
	callbackApi.RegisterRoutes()

	// 8. 鍚姩 HTTP 鐩戝惉锛岃繋鎺?Java 鍏靛洟鐨勬敞鍐?	// [Issue #6] 鎵€鏈夌鐞?API (/api/job/*, /api/callback*) 宸查€氳繃 AuthMiddleware 閴存潈
	// 娉ㄥ唽蹇冭烦鎺ュ彛鍚屾牱鍔犱笂閴存潈淇濇姢
	http.HandleFunc("/api/registry", api.AuthMiddleware(registry.ReceiveHeartbeat))
	
	listenUrl := ":" + *port
	fmt.Printf("\n鉁?NanoJob 鍚姩瀹屾垚锛佹鍦ㄧ洃鍚?%s 绔彛锛岀瓑寰呮墽琛屽櫒鎺ュ叆...\n", listenUrl)
	if err := http.ListenAndServe(listenUrl, nil); err != nil {
		panic(err)
	}
}

// fireOnce 绾补鐨勬淳鍙戦€昏緫锛屾墦瀹屼粭灏辨挙锛岀粷涓嶈嚜寰幆锛堜緵姝ｅ父瑙﹀彂鍜?Misfire 琛ュ伩澶嶇敤锛?func fireOnce(job *store.JobInfo) {
	fmt.Printf("\n[%s] 鈿?浠诲姟瑙﹀彂锛佸紑濮嬫淳鍙?-> %s\n", time.Now().Format("15:04:05"), job.ID)

	aliveNodes := registry.GetAliveNodes(job.AppName)
	if len(aliveNodes) == 0 {
		fmt.Printf("   -> 璀﹀憡锛氫笟鍔＄粍 [%s] 涓嬫病鏈夋椿鐫€鐨?Java 鏈哄櫒锛屼换鍔″彧鑳借烦杩囥€俓n", job.AppName)
		return
	}
	
	shardResults, _ := router.Route(router.StrategySharding, aliveNodes)

	for _, shard := range shardResults {
		go func(s router.ShardResult) {
			// 鎶婂瓧绗︿覆绫诲瀷鐨?job.ID 杞垚 XXL-Job 瀹㈡埛绔姹傜殑鏁板瓧绫诲瀷
			realJobID, err := strconv.Atoi(job.ID)
			if err != nil {
				// 鍏滃簳澶勭悊锛氬鏋滆В鏋愬け璐ラ粯璁や紶 0锛岄伩鍏嶇▼搴忓穿婧冦€?				fmt.Printf("   -> 鈿狅笍 璀﹀憡锛氫换鍔?ID [%s] 鏃犳硶杞崲涓烘暟瀛楃被鍨? %v\n", job.ID, err)
			}

			req := &xxljob.RunReq{
				JobID:           realJobID,
				ExecutorHandler: job.ExecutorHandler,
				GlueType:        "BEAN",
				BroadcastIndex:  s.BroadcastIndex,
				BroadcastTotal:  s.BroadcastTotal,
			}
			if err := xxljob.Trigger(s.TargetIP, req); err != nil {
				fmt.Printf("   -> 娲惧彂澶辫触 (%s): %v\n", s.TargetIP, err)
			} else {
				fmt.Printf("   -> 馃殌 鎴愬姛鍑讳腑鐩爣 %s (鍒嗙墖 %d/%d)\n", s.TargetIP, s.BroadcastIndex, s.BroadcastTotal)
			}
		}(shard)
	}
}

// scheduleJob 鏍稿績榄旀硶锛氱畻鏃堕棿銆佸鍏ヨ疆瀛愩€佽嚜鍔ㄥ惊鐜?func scheduleJob(job *store.JobInfo) {
	now := time.Now().Unix()

	// 猸愶笍 Misfire 婕忓彂琛ュ伩鏈哄埗 猸愶笍
	// 濡傛灉閰嶇疆閲屽瓨鍦ㄩ鏈熺殑鎵ц鏃堕棿锛岃€屼笖褰撳墠鏃堕棿宸茬粡瓒呰繃浜嗛鏈熸椂闂?(缁?5 绉掔綉缁滃闄愭湡),鍥犱负寤惰繜鏄父瑙佺殑,涓嶅簲鎶婁换浣曞欢杩熻涓烘紡鍙?鎵€浠ユ垜浠粰浜嗕竴涓?绉掔殑瀹介檺鏈?濡傛灉瓒呰繃5绉掑氨璁や负鏄紡鍙戜簡
	if job.NextTriggerTime > 0 {
		if job.NextTriggerTime < now-5 {
			fmt.Printf("\n[Misfire 棰勮] 鍙戠幇浠诲姟 %s 鍦ㄥ畷鏈烘湡闂存紡鍙戯紒绔嬪嵆瑙﹀彂 [FIRE_ONCE_NOW] 琛ュ伩鏈哄埗锛乗n", job.ID)
			
			// 鐙珛寮€涓€涓崗绋嬶紝绔嬪埢鎶婃紡鎺夌殑浠诲姟琛ュ彂鍑哄幓锛?绾淳鍙戯紝涓嶅共鎵板悗缁甯哥殑璋冨害寰幆)
			go fireOnce(job)
		} else if job.NextTriggerTime <= now {
			// TODO: 杞诲井杩熷埌 (0~5绉掑唴) 鎴栧垰濂藉埌鏈熴€傜洰鍓嶄唬鐮佷細鐩存帴璺宠繃褰撴鎵ц锛屽皢鍏跺畨鎺掑湪涓嬩釜鍛ㄦ湡銆?			// 鎸夌収澶у巶鏍囧噯锛岃繖閲屽簲璇ュ拰鈥滄病鏈夊欢杩熲€濅竴鏍凤紝绔嬪埢瑙﹀彂褰撴鎵ц锛岀劧鍚庡啀绠椾笅涓€娆＄殑鏃堕棿鎵旇繘鏃堕棿杞€?			// go fireOnce(job)
		}
	}

	// A. 缈昏瘧瀹樺嚭椹細绠椾竴涓嬭窛绂讳笅涓€娆℃墽琛岃繕鏈夊灏戠
	delay, err := cronParser.NextDelay(job.Cron)
	if err != nil {
		fmt.Printf("[璋冨害寮傚父] 浠诲姟 %s 鐨?Cron 瑙ｆ瀽澶辫触: %v\n", job.ID, err)
		return
	}

	// 猸愶笍 鎸佷箙鍖栬蹇?猸愶笍
	// 绠楀嚭涓嬩竴娆＄湡瀹炵殑缁濆鏃堕棿鎴筹紝骞跺紓姝ュ啓鍥?etcd锛佽繖鏍峰摢鎬曚笅涓€绉掓柇鐢碉紝绯荤粺涔熸湁璁板繂锛?	job.NextTriggerTime = time.Now().Add(delay).Unix()
	go etcdStore.SaveJob(context.Background(), job)

	// B. 瀹氫箟杩欎釜浠诲姟鈥滃埌鐐瑰悗鐪熸瑕佸共鐨勬椿鈥?	var triggerFunc func()
	triggerFunc = func() {
		// 1. 鎵撲粭
		fireOnce(job)
		// 2. 鐏甸瓊鑷惊鐜細閲嶆柊鎺掗槦锛?		scheduleJob(job)
	}

	// C. 姝ｅ紡鎶婅繖涓棴鍖呭嚱鏁帮紝鎵旇繘鏃堕棿杞帓闃?	tw.AddTask(delay, job.ID, triggerFunc)
	fmt.Printf(" -> 浠诲姟瑁呭～瀹屾瘯: %s, 棰勮 %d 绉掑悗寮曠垎\n", job.ID, int(delay.Seconds()))
}
