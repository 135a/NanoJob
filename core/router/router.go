package router

import (
	"fmt"
	"sync/atomic"
)

// rrCounter 简单的轮询计数器: 多台执行器轮流干活, 避免固定压在第一台。
var rrCounter uint64

// PickOne 从存活节点中挑一个目标 (单目标派发)。
// [砍#5] SHARDING 分片广播已移除: 每次派发固定只打一台机器,
// BroadcastIndex/Total 恒为 0/1。ROUND_ROBIN 名不副实的问题顺带修掉 —— 现在是真轮询。
func PickOne(aliveNodes []string) (string, error) {
	n := len(aliveNodes)
	if n == 0 {
		return "", fmt.Errorf("当前没有任何存活的执行器节点, 任务无法下发")
	}
	idx := int(atomic.AddUint64(&rrCounter, 1)-1) % n
	return aliveNodes[idx], nil
}
