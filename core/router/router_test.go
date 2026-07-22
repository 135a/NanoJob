package router

import (
	"testing"
)

func TestRouterSharding(t *testing.T) {
	// 假设 Registry 告诉我们目前有 3 台机器活着
	aliveNodes := []string{"192.168.1.100", "192.168.1.101", "192.168.1.102"}

	// 1. 测试：分片广播策略
	t.Log("=== 测试【分片广播 (SHARDING)】策略 ===")
	results, err := Route(StrategySharding, aliveNodes)
	if err != nil {
		t.Fatalf("路由计算失败: %v", err)
	}

	for _, res := range results {
		t.Logf("组装指令 -> IP: %s, 你负责分片: %d / %d", res.TargetIP, res.BroadcastIndex, res.BroadcastTotal)
	}
	
	if len(results) != 3 || results[2].BroadcastIndex != 2 {
		t.Fatalf("分片计算错误")
	}

	// 2. 测试：单机轮询策略
	t.Log("\n=== 测试【单机轮询 (ROUND_ROBIN)】策略 ===")
	resultsSingle, err := Route(StrategyRoundRobin, aliveNodes)
	if err != nil {
		t.Fatalf("路由计算失败: %v", err)
	}

	for _, res := range resultsSingle {
		t.Logf("组装指令 -> IP: %s, 只有你干活，你是分片: %d / %d", res.TargetIP, res.BroadcastIndex, res.BroadcastTotal)
	}
}
