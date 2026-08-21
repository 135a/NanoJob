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

// NewCronParser 创建支持秒级解析的 Cron 解析器。
// XXL-Job 兼容 Spring 体系, 使用 6 位 (精确到秒) 的表达式, 须显式开启 cron.Second。
func NewCronParser() *CronParser {
	return &CronParser{
		parser: cron.NewParser(
			cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		),
	}
}

// NextDelay 返回距离下一次执行的等待时间。
func (p *CronParser) NextDelay(cronExpr string) (time.Duration, error) {
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return 0, fmt.Errorf("非法的 Cron 表达式 [%s]: %v", cronExpr, err)
	}

	now := time.Now()
	nextTime := schedule.Next(now)

	// 警戒: 若计算出的时间反而在过去, 说明表达式导致异常
	if nextTime.Before(now) {
		return 0, fmt.Errorf("调度异常：算出的下次执行时间竟然在过去？")
	}

	return nextTime.Sub(now), nil
}
