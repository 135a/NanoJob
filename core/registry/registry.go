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
