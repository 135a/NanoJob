package timewheel

import (
	"sync"
	"time"
)

// Task 代表挂载在时间轮上的一个定时任务
type Task struct {
	ID       string // 任务的唯一标识
	Circle   int    // 重点：剩余的圈数
	Execute  func() // 任务触发时真正要执行的回调函数
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

	// 1. 计算总共需要多少个“滴答”
	ticks := int(delay / tw.interval)
	
	// 2. 计算需要转几圈
	circle := ticks / tw.slotNum
	
	// 3. 计算应该落在哪一个槽位 (当前位置 + 余数)
	pos := (tw.current + ticks) % tw.slotNum

	// 4. 构建任务并放入对应的槽位
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

	// 遍历当前槽位的所有任务
	for _, task := range tasks {
		if task.Circle > 0 {
			// 如果圈数没走完，圈数减1，继续留在槽位里等待下一圈
			task.Circle--
			remainingTasks = append(remainingTasks, task)
		} else {
			// 如果圈数为 0，说明时间到了，立刻异步执行任务！
			go task.Execute()
		}
	}

	// 更新当前槽位（剔除掉已经执行的任务）
	tw.slots[tw.current] = remainingTasks
	
	// 指针往前走一格
	tw.current = (tw.current + 1) % tw.slotNum
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	close(tw.stopChan)
}
