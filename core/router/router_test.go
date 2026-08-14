package router

import (
	"testing"
)

func TestPickOne(t *testing.T) {
	nodes := []string{"192.168.1.100:9999", "192.168.1.101:9999", "192.168.1.102:9999"}

	// 1. 选中的目标必须在存活列表里
	target, err := PickOne(nodes)
	if err != nil {
		t.Fatalf("路由失败: %v", err)
	}
	t.Logf("选中目标: %s", target)
	found := false
	for _, n := range nodes {
		if n == target {
			found = true
		}
	}
	if !found {
		t.Fatalf("选中的目标不在存活列表里: %s", target)
	}

	// 2. 连续调用应轮流 (轮询)
	seen := map[string]int{}
	for i := 0; i < len(nodes)*3; i++ {
		tgt, _ := PickOne(nodes)
		seen[tgt]++
	}
	t.Logf("轮询分布: %v", seen)
	for n, c := range seen {
		if c < 2 {
			t.Fatalf("节点 %s 只被选中 %d 次, 不像轮询", n, c)
		}
	}

	// 3. 空节点列表必须报错
	if _, err := PickOne(nil); err == nil {
		t.Fatal("空节点列表应该报错")
	}
}
