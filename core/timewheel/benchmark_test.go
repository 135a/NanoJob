package timewheel

import (
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkTimeWheelAdd 并发添加任务吞吐：测量时间轮插入任务的速率
func BenchmarkTimeWheelAdd(b *testing.B) {
	tw := New(time.Millisecond, 3600)
	delay := time.Hour // 长延迟，确保基准测量期间任务不会触发
	var id atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := id.Add(1)
			tw.AddTask(delay, strconv.FormatInt(n, 10), func() {})
		}
	})
	b.ReportAllocs()
}

// TestSchedulingPrecision 调度精度：统计实际触发时间与预期触发时间的偏差
// 2000 个任务，延迟 2s~4s 均匀分布，1ms 粒度时间轮
func TestSchedulingPrecision(t *testing.T) {
	const count = 2000
	tw := New(time.Millisecond, 3600)
	tw.Start()
	defer tw.Stop()

	expected := make([]time.Time, count)
	actual := make([]time.Time, count)
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		d := time.Duration(2000+i) * time.Millisecond
		expected[i] = time.Now().Add(d)
		tw.AddTask(d, strconv.Itoa(i), func() {
			actual[i] = time.Now()
			wg.Done()
		})
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("任务未在超时内全部触发")
	}

	devs := make([]time.Duration, count)
	var sum time.Duration
	var max time.Duration
	for i := 0; i < count; i++ {
		dev := actual[i].Sub(expected[i])
		if dev < 0 {
			dev = -dev
		}
		devs[i] = dev
		sum += dev
		if dev > max {
			max = dev
		}
	}
	sort.Slice(devs, func(x, y int) bool { return devs[x] < devs[y] })
	avg := sum / time.Duration(count)
	p95 := devs[int(float64(count)*0.95)-1]
	t.Logf("调度 %d 个任务：平均偏差 %v / P95 偏差 %v / 最大偏差 %v", count, avg, p95, max)
}

// TestTriggerThroughput 触发吞吐：50,000 个任务全部触发所需耗时与速率
func TestTriggerThroughput(t *testing.T) {
	const count = 50000
	tw := New(time.Millisecond, 3600)
	tw.Start()
	defer tw.Stop()

	var fired atomic.Int64
	for i := 0; i < count; i++ {
		d := time.Duration(500+i%3000) * time.Millisecond // 0.5s ~ 3.5s 均匀分布
		tw.AddTask(d, strconv.Itoa(i), func() { fired.Add(1) })
	}
	start := time.Now()

	deadline := time.After(15 * time.Second)
	for fired.Load() < count {
		select {
		case <-deadline:
			t.Fatalf("超时，仅触发 %d/%d", fired.Load(), count)
		case <-time.After(20 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d 个任务全部触发，总耗时 %v，触发速率 %.0f 任务/秒", count, elapsed, float64(count)/elapsed.Seconds())
}

// TestMemoryFootprint 挂载 100 万个任务的内存占用
func TestMemoryFootprint(t *testing.T) {
	const count = 1000000
	tw := New(time.Millisecond, 3600)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < count; i++ {
		tw.AddTask(time.Hour, strconv.Itoa(i), func() {})
	}
	runtime.ReadMemStats(&after)
	used := after.HeapAlloc - before.HeapAlloc
	t.Logf("挂载 %d 个任务，堆内存增加 %.1f MB（单任务平均 %.0f B）", count, float64(used)/(1024*1024), float64(used)/float64(count))
}
