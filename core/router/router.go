package router

import (
	"fmt"
)

// RouteStrategy 路由策略定义
type RouteStrategy string

const (
	StrategyRoundRobin RouteStrategy = "ROUND_ROBIN" // 轮询单机执行
	StrategySharding   RouteStrategy = "SHARDING"    // 分片广播执行 (海量跑批核心)
)

// ShardResult 路由计算出的作战任务书
type ShardResult struct {
	TargetIP       string // 枪口对准谁
	BroadcastIndex int    // 告诉他干第几片的活
	BroadcastTotal int    // 告诉他总共有几片
}

// Route 核心大脑：根据策略和当前活着的节点，计算出最终的派发清单
func Route(strategy RouteStrategy, aliveNodes []string) ([]ShardResult, error) {
	if len(aliveNodes) == 0 {
		return nil, fmt.Errorf("当前没有任何存活的执行器节点，任务无法下发")
	}

	var results []ShardResult
	total := len(aliveNodes)

	switch strategy {
	case StrategySharding:
		// 【分片广播】：给每一个存活的节点都分配一个序号
		for index, ip := range aliveNodes {
			results = append(results, ShardResult{
				TargetIP:       ip,
				BroadcastIndex: index,
				BroadcastTotal: total,
			})
		}
		
	case StrategyRoundRobin:
		// 【轮询单机】：从所有节点里挑 1 个干活。
		// 这里为了极简演示，固定挑第一个（生产中会用全局原子计数器取模）
		results = append(results, ShardResult{
			TargetIP:       aliveNodes[0],
			BroadcastIndex: 0,
			BroadcastTotal: 1, // 因为只挑了 1 台机器，所以总分片就是 1
		})
		
	default:
		return nil, fmt.Errorf("不支持的路由策略: %s", strategy)
	}

	return results, nil
}
