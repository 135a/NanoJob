package timewheel

import (
	"sync"
	"time"
)

// Task 挂载在时间轮上的一个定时任务
type Task struct {
	ID      string
	Circle  int    // 剩余圈数: 降到 0 才触发
	Execute func() // 触发时执行的回调
}

type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    [][]*Task
	current  int
	slotNum  int
	mutex    sync.RWMutex
	stopChan chan struct{}
}

// New 实例化一个时间轮
func New(interval time.Duration, slotNum int) *TimeWheel {
	return &TimeWheel{
		interval: interval,
		slots:    make([][]*Task, slotNum),
		current:  0,
		slotNum:  slotNum,
		stopChan: make(chan struct{}),
	}
}

// Start 启动时间轮的心跳
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.run()
}

// AddTask 计算圈数和槽位，将任务插入时间轮
func (tw *TimeWheel) AddTask(delay time.Duration, id string, execute func()) {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	// 总滴答数 → 圈数 → 最终槽位 (当前位置 + 余数)
	ticks := int(delay / tw.interval)
	circle := ticks / tw.slotNum
	pos := (tw.current + ticks) % tw.slotNum

	task := &Task{
		ID:      id,
		Circle:  circle,
		Execute: execute,
	}
	tw.slots[pos] = append(tw.slots[pos], task)
}

// run 时间轮心脏
func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case <-tw.stopChan:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	tasks := tw.slots[tw.current]
	var remainingTasks []*Task

	for _, task := range tasks {
		if task.Circle > 0 {
			task.Circle-- // 圈数未走完, 继续留在槽位等待下一圈
			remainingTasks = append(remainingTasks, task)
		} else {
			go task.Execute() // 圈数为 0, 时间到, 异步执行
		}
	}

	tw.slots[tw.current] = remainingTasks      // 剔除已执行任务后回填
	tw.current = (tw.current + 1) % tw.slotNum // 指针前移一格
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	close(tw.stopChan)
}
