package timewheel

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// firedCollector 收集被触发任务的 ID, 并同步等待所有异步回调跑完 (tickHandler 用 go 派发)。
type firedCollector struct {
	mu  sync.Mutex
	ids []string
	wg  sync.WaitGroup
}

// onFire 生成一个任务回调: 触发时记录 id。必须在 AddTask 前调用 (wg.Add 先于可能的 Done)。
func (c *firedCollector) onFire(id string) func() {
	c.wg.Add(1)
	return func() {
		c.mu.Lock()
		c.ids = append(c.ids, id)
		c.mu.Unlock()
		c.wg.Done()
	}
}

// waitBounded 等所有触发回调跑完并返回已触发 id; 超时返回 nil。
func (c *firedCollector) waitBounded(timeout time.Duration) []string {
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		c.mu.Lock()
		defer c.mu.Unlock()
		out := make([]string, len(c.ids))
		copy(out, c.ids)
		return out
	case <-time.After(timeout):
		return nil
	}
}

// advance 手动推进 n 次 tick (确定性: 不依赖真实 ticker, 由测试自己"拨钟")。
func advance(tw *TimeWheel, n int) {
	for i := 0; i < n; i++ {
		tw.tickHandler()
	}
}

// ---- 槽位与圈数计算 (白盒) ----

func TestAddTaskSlotAndCircle(t *testing.T) {
	cases := []struct {
		name       string
		delay      time.Duration
		wantPos    int
		wantCircle int
	}{
		{"不满一圈", 3 * time.Second, 3, 0},
		{"正好一圈", 10 * time.Second, 0, 1}, // 10s=1圈 → 落回当前槽, circle=1
		{"一圈加2秒", 12 * time.Second, 2, 1},
		{"两圈加3秒", 23 * time.Second, 3, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tw := New(1*time.Second, 10) // interval 1s × 10 槽 = 一圈 10s
			tw.AddTask(c.delay, "job", func() {})
			tw.mutex.Lock()
			slot := tw.slots[c.wantPos]
			tw.mutex.Unlock()
			if len(slot) != 1 {
				t.Fatalf("delay=%v 应落在 slot %d, 该槽实际有 %d 个任务", c.delay, c.wantPos, len(slot))
			}
			if slot[0].Circle != c.wantCircle {
				t.Fatalf("delay=%v 应 circle=%d, 实际 %d", c.delay, c.wantCircle, slot[0].Circle)
			}
		})
	}
}

// ---- 触发时机 (确定性手动拨钟) ----
// 触发语义: 任务在加入后第 ticks+1 次 tick 触发 (ticks = delay/interval)。
// 即最小延迟为 1 个 interval, 不存在"加入即触发"。

func TestFiresAfterExpectedTicks(t *testing.T) {
	tw := New(1*time.Second, 10)
	col := &firedCollector{}
	tw.AddTask(3*time.Second, "job-3s", col.onFire("job-3s")) // ticks=3 → 第 4 次 tick 触发

	advance(tw, 3)
	if got := col.waitBounded(time.Second); len(got) != 0 {
		t.Fatalf("第 3 次 tick 不应触发, 实际: %v", got)
	}
	advance(tw, 1)
	if got := col.waitBounded(time.Second); len(got) != 1 || got[0] != "job-3s" {
		t.Fatalf("应在第 4 次 tick 触发, 实际: %v", got)
	}
}

func TestMultiTurnFiring(t *testing.T) {
	tw := New(1*time.Second, 5) // 一圈 5s
	col := &firedCollector{}
	tw.AddTask(12*time.Second, "job-12s", col.onFire("job-12s")) // ticks=12, circle=2, pos=2

	// slot2 在第 3、8、13 次 tick 被处理; circle 2→1→0, 第 13 次触发
	advance(tw, 12)
	if got := col.waitBounded(time.Second); len(got) != 0 {
		t.Fatalf("跨圈任务第 12 次 tick 前不应触发, 实际: %v", got)
	}
	advance(tw, 1)
	if got := col.waitBounded(time.Second); len(got) != 1 {
		t.Fatalf("跨圈任务应在第 13 次 tick 触发, 实际: %v", got)
	}
}

// TestAddDuringRotation 轮子转到中途再加任务, 位置计算必须仍指向正确未来槽位。
func TestAddDuringRotation(t *testing.T) {
	tw := New(1*time.Second, 10)
	col := &firedCollector{}

	advance(tw, 4) // 当前已走 4 格 (current=4)
	tw.AddTask(3*time.Second, "job-mid", col.onFire("job-mid")) // ticks=3, pos=(4+3)%10=7

	// 第 7 次 tick 处理 slot6, 不应触发; 第 8 次 tick 处理 slot7 触发
	advance(tw, 3)
	if got := col.waitBounded(time.Second); len(got) != 0 {
		t.Fatalf("中途添加的任务过早触发, 实际: %v", got)
	}
	advance(tw, 1)
	if got := col.waitBounded(time.Second); len(got) != 1 {
		t.Fatalf("中途添加的任务应在预期时刻触发, 实际: %v", got)
	}
}

// TestSubIntervalDelayFiresNextTick 已知特性: delay < interval 的任务在下一个 tick 触发,
// 最小延迟 = 1 个 interval (不会提前, 也不会有更精确的触发点)。
func TestSubIntervalDelayFiresNextTick(t *testing.T) {
	tw := New(10*time.Millisecond, 5)
	col := &firedCollector{}
	tw.AddTask(3*time.Millisecond, "sub", col.onFire("sub")) // ticks=0 → 下一个 tick 触发

	advance(tw, 1)
	if got := col.waitBounded(100 * time.Millisecond); len(got) != 1 {
		t.Fatalf("子 interval 任务应在下一个 tick 触发, 实际: %v", got)
	}
}

// ---- 模型对照测试 (不造数据: 断言时间轮行为与参考模型完全一致) ----
// 参考模型: 任务在第 (加入时的已走tick数 + delay/interval + 1) 次 tick 触发。
// 随机灌入"加任务 / 拨钟"操作序列, 跑完后校验: 每个任务恰好触发一次、无多余触发。

func TestModelBasedFiring(t *testing.T) {
	rng := rand.New(rand.NewSource(42)) // 固定种子, 可复现
	const slotNum = 11
	tw := New(time.Millisecond, slotNum) // interval 1ms
	col := &firedCollector{}

	type entry struct {
		id       string
		fireTick int
	}
	var expected []entry

	m := 0 // 已完成的 tick 数
	for step := 0; step < 300; step++ {
		switch rng.Intn(3) {
		case 0, 1: // 加任务
			delayMs := rng.Intn(500)
			id := fmt.Sprintf("t%d-%d", step, delayMs)
			tw.AddTask(time.Duration(delayMs)*time.Millisecond, id, col.onFire(id))
			expected = append(expected, entry{id, m + delayMs + 1})
		case 2: // 拨钟
			m++
			advance(tw, 1)
		}
	}
	// 拨足剩余 tick, 让所有任务必然到期
	advance(tw, 600)
	m += 600

	got := col.waitBounded(3 * time.Second)
	if len(got) != len(expected) {
		t.Fatalf("触发数 %d != 预期 %d (有任务丢失或重复触发)", len(got), len(expected))
	}

	// 校验: 每个 id 都恰好触发一次 (id 唯一, 计数相等即不重不漏)
	sort.Slice(expected, func(i, j int) bool { return expected[i].id < expected[j].id })
	sort.Strings(got)
	for i := range expected {
		if got[i] != expected[i].id {
			t.Fatalf("触发集合不匹配: 期望 %s, 实际 %s", expected[i].id, got[i])
		}
	}
}

// ---- 真实定时器 (端到端, 容忍度断言) ----

func TestFiresRealTime(t *testing.T) {
	tw := New(50*time.Millisecond, 20) // 50ms tick, 一圈 1s
	tw.Start()
	defer tw.Stop()

	col := &firedCollector{}
	start := time.Now()
	// delay=300ms → ticks=6 → 实际 ~350ms 触发 (300 + 1 tick)
	tw.AddTask(300*time.Millisecond, "job", col.onFire("job"))

	if got := col.waitBounded(2 * time.Second); len(got) != 1 {
		t.Fatalf("真实定时器未触发: %v", got)
	}
	elapsed := time.Since(start)
	if elapsed < 300*time.Millisecond || elapsed > 600*time.Millisecond {
		t.Fatalf("真实触发延迟 %v 超出预期窗口 [300ms, 600ms]", elapsed)
	}
}

// ---- 并发安全 (-race 下验证 AddTask 与 tickHandler 无数据竞争) ----

func TestConcurrentAddAndTick(t *testing.T) {
	tw := New(time.Millisecond, 8)
	col := &firedCollector{}

	stop := make(chan struct{})
	var tickWG sync.WaitGroup
	tickWG.Add(1)
	go func() { // 模拟轮子心跳: 持续拨钟
		defer tickWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				tw.tickHandler()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	const n = 200
	var addWG sync.WaitGroup
	addWG.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer addWG.Done()
			tw.AddTask(time.Duration(i%8)*time.Millisecond, fmt.Sprintf("job-%d", i), col.onFire(fmt.Sprintf("job-%d", i)))
		}(i)
	}
	addWG.Wait()                      // 所有任务已入轮
	time.Sleep(30 * time.Millisecond) // 再拨 30ms, 让全部到期任务触发
	close(stop)
	tickWG.Wait()

	got := col.waitBounded(2 * time.Second)
	if len(got) != n {
		t.Fatalf("并发下所有任务都应触发, 实际 %d/%d", len(got), n)
	}
}

// ---- Stop 后不再触发 ----

func TestStopStopsWheel(t *testing.T) {
	tw := New(10*time.Millisecond, 10)
	col := &firedCollector{}
	tw.AddTask(10*time.Millisecond, "job", col.onFire("job"))

	tw.Start()
	tw.Stop() // 立即停
	time.Sleep(50 * time.Millisecond)

	if got := col.waitBounded(50 * time.Millisecond); len(got) != 0 {
		t.Fatalf("Stop 后不应再触发任何任务, 实际: %v", got)
	}
}
