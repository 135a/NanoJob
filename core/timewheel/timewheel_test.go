package timewheel

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeWheel(t *testing.T) {
	// 1. 创建一个 1秒滴答一次，总共 10 个槽位的时间轮 (意味着 10秒转完一圈)
	tw := New(1*time.Second, 10)
	tw.Start()
	defer tw.Stop()

	fmt.Println("时间轮启动！当前时间:", time.Now().Format("15:04:05"))

	// 2. 添加一个 3 秒后执行的任务 (不满一圈)
	tw.AddTask(3*time.Second, "job-3s", func() {
		fmt.Printf("[%s] 任务触发！我是 3 秒任务\n", time.Now().Format("15:04:05"))
	})

	// 3. 添加一个 12 秒后执行的任务 (大于 10，所以引擎会自动标记为 Circle=1, 槽位=2)
	tw.AddTask(12*time.Second, "job-12s", func() {
		fmt.Printf("[%s] 任务触发！我是 12 秒任务（我耐心地等完了一圈）\n", time.Now().Format("15:04:05"))
	})

	// 阻塞等待 15 秒，看它们能不能准时打印出来
	time.Sleep(15 * time.Second)
	fmt.Println("时间轮测试圆满结束。")
}
