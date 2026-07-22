package timewheel

import (
	"sync"
	"time"
)

// Task 代表挂载在时间轮上的一个定时任务
type Task struct {
	ID       string      // 任务的唯一标识
	Execute  func()      // 任务触发时真正要执行的回调函数 (比如发送 HTTP 请求给 Java)
}

// TimeWheel 内存时间轮核心结构
type TimeWheel struct {
	interval time.Duration // 时钟滴答的间隔（例如 1 秒滴答一次）
	ticker   *time.Ticker  // Go 标准库底层的硬件级定时器
	slots    [][]*Task     // 环形数组：时间轮的每一个槽位 (里面存着待触发的任务列表)
	current  int           // 当前指针指在哪个槽位
	slotNum  int           // 槽位的总数量（例如 60 个槽代表一分钟）
	
	mutex    sync.RWMutex  // 互斥锁，保证并发添加任务时的内存安全
	stopChan chan struct{} // 用于优雅停机的信号通道
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
	go tw.run() // 开启一个 Goroutine 在后台静默运行
}

// run 时间轮的死循环心脏（注意：这是纯内存运转，不查数据库）
func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler() // 每次滴答，指针往前走一格
		case <-tw.stopChan:
			tw.ticker.Stop()
			return
		}
	}
}

// tickHandler 处理每一次指针的移动
func (tw *TimeWheel) tickHandler() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	// 1. 拿到当前指针槽位里的所有任务
	tasks := tw.slots[tw.current]
	
	// 2. 遍历触发这些任务 (丢给其他的 Goroutine 异步执行，绝不阻塞当前时间轮指针)
	for _, task := range tasks {
		go task.Execute() 
	}

	// 3. 清空当前槽位，并且指针往前走一格（取模实现环形运转）
	tw.slots[tw.current] = make([]*Task, 0)
	tw.current = (tw.current + 1) % tw.slotNum
}
