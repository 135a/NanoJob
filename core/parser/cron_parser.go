package parser

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// CronParser 封装了对 Cron 表达式的精确解析逻辑
type CronParser struct {
	parser cron.Parser
}

// NewCronParser 创建一个支持【秒级解析】的 Cron 翻译官
func NewCronParser() *CronParser {
	// XXL-Job 兼容 Spring 体系，默认使用 6 位的 Cron 表达式 (精确到秒)
	// 我们必须显式开启 cron.Second 支持，否则解析会报错
	return &CronParser{
		parser: cron.NewParser(
			cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		),
	}
}

// NextDelay 核心计算法术：告诉你距离下一次执行，到底还要苦等多少秒？
func (p *CronParser) NextDelay(cronExpr string) (time.Duration, error) {
	// 1. 解析字符串规则
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return 0, fmt.Errorf("非法的 Cron 表达式 [%s]: %v", cronExpr, err)
	}

	now := time.Now()
	// 2. 预测下一次触发的绝对时间点
	nextTime := schedule.Next(now)
	
	// 如果因为某些诡异规则算出来的时间在过去，直接拦截
	if nextTime.Before(now) {
		return 0, fmt.Errorf("调度异常：算出的下次执行时间竟然在过去？")
	}

	// 3. 绝对时间 - 当前时间 = 需要等待的 Duration (时间轮最喜欢的参数格式)
	return nextTime.Sub(now), nil
}
