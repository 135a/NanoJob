package parser

import (
	"testing"
	"time"
)

func TestCronParser(t *testing.T) {
	parser := NewCronParser()

	// 模拟一条 XXL-Job 里最典型的跑批规则：每隔 10 秒跑一次
	// 注意这是 6 位的格式，第一位代表秒
	cronExpr := "*/10 * * * * *"
	
	delay, err := parser.NextDelay(cronExpr)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}

	t.Logf("【当前系统时刻】: %s", time.Now().Format("15:04:05"))
	t.Logf("【数据库配置的 Cron】: %s", cronExpr)
	t.Logf("【翻译官计算结果】: 你需要让这颗任务在时间轮里沉睡 %v", delay)
}
