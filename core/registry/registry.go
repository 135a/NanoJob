package registry

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// AppRegistry 存储某一个应用（如 loan-service）下面所有存活的节点 IP
type AppRegistry struct {
	mu    sync.RWMutex
	nodes map[string]time.Time // IP -> 最后一次心跳时间
}

// TODO: [架构缺陷 5] 状态孤岛问题 (Stateful Registry)
// 致命 Bug 预警：当前心跳注册表 globalRegistry 是纯内存 Map！
// 当 K8s 部署多台 Go 引擎时，负载均衡会导致各个 Go 引擎只收到部分 Java 节点的心跳。
// 结果：在分片广播模式下，每台 Go 引擎只会把任务下发给它自认为活着的节点，导致严重的分片不均，
// 甚至结合缺乏选主的 Bug，会造成多个节点重复全量执行业务，引发严重的数据灾难！
//
// [终极解决方案]：全面拥抱 etcd (待未来重构)
// 1. 服务发现 (Service Discovery)：废弃内存 Map，Go 引擎收到心跳后直接写入 etcd 并绑定 90 秒 Lease（租约）。
//    - Key 格式：/nanojob/registry/{appname}/{ip:port}
//    - etcd 将自动利用 Lease TTL 处理节点的过期剔除，彻底干掉手写的 Monitor 轮询。
//    - 派发任务时，Go 引擎实时向 etcd 发起 Prefix 查询，确保任何一台引擎拿到的分片名单都是 100% 全局一致的！
// 2. 分布式抢锁选主 (Leader Election)：使用 etcd 的 concurrency.NewMutex()。
//    - 只有抢到锁的 Go 引擎（Leader）才有资格转动 TimeWheel 派发任务。
//    - 另外两台 Go 引擎作为 Standby。一旦 Leader 宕机，租约失效，立刻会有新的 Standby 抢锁上位，实现无缝故障转移！

var (
	// 全局注册表: AppName -> AppRegistry
	globalRegistry = make(map[string]*AppRegistry)
	registryMutex  sync.RWMutex
)

// RegistryParam XXL-Job 客户端心跳上报的 JSON 请求格式
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"` // 通常是 "EXECUTOR"
	RegistryKey   string `json:"registryKey"`   // 应用名，比如 "loan-service"
	RegistryValue string `json:"registryValue"` // 节点的 IP:Port (如 192.168.1.100:9999)
}

// ReceiveHeartbeat 提供给 Java 端调用的 HTTP 接口处理函数
func ReceiveHeartbeat(w http.ResponseWriter, r *http.Request) {
	var param RegistryParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		http.Error(w, "解析心跳报文失败", http.StatusBadRequest)
		return
	}

	registryMutex.Lock()
	appReg, exists := globalRegistry[param.RegistryKey]
	if !exists {
		// 如果这个应用是第一次来注册，帮它建一个专属名单库
		appReg = &AppRegistry{
			nodes: make(map[string]time.Time),
		}
		globalRegistry[param.RegistryKey] = appReg
	}
	registryMutex.Unlock()

	// 核心操作：更新这台机器的【最后存活时间】为当前时刻
	appReg.mu.Lock()
	appReg.nodes[param.RegistryValue] = time.Now()
	appReg.mu.Unlock()

	// 按 XXL-Job 协议返回 200 OK
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"code": 200, "msg": null}`))
}

// GetAliveNodes （供我们的 Router 调用）获取某个应用下当前活着的所有节点 IP
func GetAliveNodes(appname string) []string {
	registryMutex.RLock()
	appReg, exists := globalRegistry[appname]
	registryMutex.RUnlock()

	if !exists {
		return nil
	}

	var aliveNodes []string
	appReg.mu.RLock()
	for ip, lastBeat := range appReg.nodes {
		// 防御性判断：如果距离上次心跳没有超过 90 秒，认为它是活的
		if time.Since(lastBeat) < 90*time.Second {
			aliveNodes = append(aliveNodes, ip)
		}
	}
	appReg.mu.RUnlock()

	return aliveNodes
}

// StartMonitor 启动后台清道夫（定期把掉线的节点彻底从 map 里踢掉，防止内存泄漏）
func StartMonitor() {
	go func() {
		for {
			time.Sleep(30 * time.Second) // 每 30 秒巡逻一次
			
			registryMutex.RLock()
			for _, appReg := range globalRegistry {
				appReg.mu.Lock()
				for ip, lastBeat := range appReg.nodes {
					if time.Since(lastBeat) > 90*time.Second {
						// 超过 90 秒没心跳，说明机器宕机，无情踢出名单
						delete(appReg.nodes, ip) 
					}
				}
				appReg.mu.Unlock()
			}
			registryMutex.RUnlock()
		}
	}()
}
