package election

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// nopLogger 静默 go-redis 默认日志: 断连测试会刻意制造连接失败, 不必刷屏。
type nopLogger struct{}

func (nopLogger) Printf(context.Context, string, ...interface{}) {}

func TestMain(m *testing.M) {
	redis.SetLogger(nopLogger{})
	os.Exit(m.Run())
}

// newTestElection 起一个指向 miniredis 的选举器 (测试不依赖真实 Redis, CI 可跑)。
func newTestElection(srv *miniredis.Miniredis, nodeID string, ttl time.Duration) *Election {
	return New(redis.NewClient(&redis.Options{Addr: srv.Addr()}), "nanojob:election:test", nodeID, ttl)
}

func mustAcquire(t *testing.T, e *Election) {
	t.Helper()
	if err := e.acquire(); err != nil {
		t.Fatalf("acquire 失败: %v", err)
	}
	if !e.IsMaster() {
		t.Fatal("acquire 成功但 IsMaster()==false")
	}
}

// drainChanges 清空 changes 通道, 避免上一次广播的历史事件干扰后续断言。
func drainChanges(e *Election) {
	for {
		select {
		case <-e.changes:
		default:
			return
		}
	}
}

// waitMaster 轮询等待 e 上位, 超时返回 false。
func waitMaster(e *Election, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.IsMaster() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitNotMaster 轮询等待 e 让位, 超时返回 false。
func waitNotMaster(e *Election, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !e.IsMaster() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ---- 抢锁 ----

func TestAcquireSuccess(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(srv, "A", 5*time.Second)
	mustAcquire(t, e)

	// 上位应广播 true
	select {
	case v := <-e.changes:
		if !v {
			t.Fatal("上位应广播 true")
		}
	default:
		t.Fatal("acquire 成功后应广播上位事件")
	}

	// 锁确实落在 Redis, 且值是本节点
	if got, _ := e.redis.Get(context.Background(), e.key).Result(); got != "A" {
		t.Fatalf("锁值应为 A, 实际 %q", got)
	}
}

func TestAcquireFailsWhenHeld(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	a := newTestElection(srv, "A", 5*time.Second)
	b := newTestElection(srv, "B", 5*time.Second)
	mustAcquire(t, a)

	// 锁被 A 持有时, B 抢锁不应报错, 只是抢不到、保持待命
	if err := b.acquire(); err != nil {
		t.Fatalf("acquire 不应报错(只是抢不到): %v", err)
	}
	if b.IsMaster() {
		t.Fatal("锁被 A 持有, B 不应上位")
	}
}

// TestSingleLeaderUnderRace 并发抢锁不变量: 任意时刻至多一个 Leader。配 -race 跑。
func TestSingleLeaderUnderRace(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	const n = 8
	errs := make(chan error, n)
	var masters int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := newTestElection(srv, fmt.Sprintf("node-%d", i), 5*time.Second)
			if err := e.acquire(); err != nil {
				errs <- err
				return
			}
			if e.IsMaster() {
				atomic.AddInt64(&masters, 1)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if masters != 1 {
		t.Fatalf("并发抢锁应恰好一个 Leader, 实际 %d 个", masters)
	}
}

// ---- 续期 ----

func TestRenewExtendsTTL(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(srv, "A", 1*time.Second)
	mustAcquire(t, e)
	ctx := context.Background()

	// miniredis 不按墙钟走时, 用 FastForward 推进虚拟时钟: 1000ms 租约剩 ~300ms
	srv.FastForward(700 * time.Millisecond)
	before, err := e.redis.PTTL(ctx, e.key).Result() // PTTL 毫秒级, 亚秒租约也能读准
	if err != nil {
		t.Fatalf("读 TTL 失败: %v", err)
	}

	if err := e.renew(); err != nil {
		t.Fatalf("renew 失败: %v", err)
	}
	after, err := e.redis.PTTL(ctx, e.key).Result()
	if err != nil {
		t.Fatalf("读 TTL 失败: %v", err)
	}

	if !(after > before) {
		t.Fatalf("续期应延长租约: before=%v after=%v", before, after)
	}
	if !e.IsMaster() {
		t.Fatal("持有锁时正常续期不应让位")
	}
}

// TestRenewDetectsStolenLock 防脑裂核心: 锁被新主接管后, 旧主 renew 必须让位。
func TestRenewDetectsStolenLock(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	a := newTestElection(srv, "A", 5*time.Second)
	mustAcquire(t, a)
	drainChanges(a)

	// 模拟锁被新主接管: 直接把锁值改成 B
	if err := a.redis.Set(context.Background(), a.key, "B", 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	if err := a.renew(); err != nil {
		t.Fatalf("renew 不应报错: %v", err)
	}
	if a.IsMaster() {
		t.Fatal("锁已被接管, renew 必须让位 (防脑裂)")
	}
	select {
	case v := <-a.changes:
		if v {
			t.Fatal("让位应广播 false")
		}
	default:
		t.Fatal("让位后应广播降级事件")
	}
}

// TestRenewErrorKeepsMaster 契约: 续期遇到 Redis 连接错误只返回错误、不降级。
// 设计取舍: Redis 不可达时谁也抢不到锁 (SetNX 同样连不上), 不会产生两个"可验证的主";
// 瞬时抖动不触发选主翻转, Redis 恢复后下一次续期由 res==-1 检查自动收敛。
// 真正需要降级的是"锁被抢走/过期" (res==-1), 见 TestRenewDetectsStolenLock。
func TestRenewErrorKeepsMaster(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}

	e := newTestElection(srv, "A", 5*time.Second)
	mustAcquire(t, e)

	srv.Close() // 模拟 Redis 宕机/网络分区: 之后所有命令报错
	if err := e.renew(); err == nil {
		t.Fatal("预期 renew 报错 (Redis 已断), 实际返回 nil")
	}
	if !e.IsMaster() {
		t.Fatal("瞬时续期错误不应触发选主翻转: 应保持主, 待 Redis 恢复后由 res==-1 收敛")
	}
}

// ---- 写前校验 ----

func TestVerifyLeadership(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(srv, "A", 5*time.Second)
	mustAcquire(t, e)
	ctx := context.Background()

	if !e.VerifyLeadership(ctx) {
		t.Fatal("持锁时 VerifyLeadership 应为 true")
	}
	// 锁被接管
	if err := e.redis.Set(ctx, e.key, "B", 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if e.VerifyLeadership(ctx) {
		t.Fatal("锁被接管后 VerifyLeadership 必须为 false")
	}
	// 锁消失
	if err := e.redis.Del(ctx, e.key).Err(); err != nil {
		t.Fatal(err)
	}
	if e.VerifyLeadership(ctx) {
		t.Fatal("锁消失后 VerifyLeadership 必须为 false")
	}
}

// ---- 循环驱动 ----

// TestLoopElectStepsDownWhenLockLost 循环端到端: 持主期间锁被外部接管,
// 循环的下一次续期必须发现锁值不是自己并让位 —— 验证 ticker→renew→降级 整条链路。
// 注: "循环持续续期保租约" 依赖真实时钟, miniredis 不走墙钟测不了, 需真 Redis 集成测试。
func TestLoopElectStepsDownWhenLockLost(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(srv, "A", 600*time.Millisecond) // tick = ttl/3 = 200ms
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 10)
	go e.LoopInElect(ctx, errCh)
	defer e.StopElect()

	if !waitMaster(e, 2*time.Second) {
		t.Fatal("LoopInElect 启动后未能抢到锁")
	}
	drainChanges(e)

	// 模拟锁被新主接管 (旧主失联期间 lease 过期, 新主抢走)
	if err := e.redis.Set(context.Background(), e.key, "B", 600*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}

	// 循环下一次续期 (≤200ms) 必须发现锁值不是自己 → 让位
	if !waitNotMaster(e, 2*time.Second) {
		t.Fatal("锁被接管后循环未让位 → 双主窗口")
	}
	select {
	case v := <-e.changes:
		if v {
			t.Fatal("让位应广播 false")
		}
	default:
		t.Fatal("让位后应广播降级事件")
	}
}
