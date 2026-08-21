package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var redisClient *redis.Client

// Init 注入 Redis 客户端
func Init(client *redis.Client) {
	redisClient = client
}

// RegistryParam XXL-Job 客户端心跳上报的 JSON 请求格式
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"` // 组, 通常为 "EXECUTOR"
	RegistryKey   string `json:"registryKey"`   // 应用名, 如 "loan-service"
	RegistryValue string `json:"registryValue"` // 节点地址 IP:Port
}

const (
	registryPrefix = "nanojob:registry:"
	heartbeatTTL   = 90 * time.Second
)

// ReceiveHeartbeat 提供给 Java 端调用的 HTTP 接口处理函数
func ReceiveHeartbeat(w http.ResponseWriter, r *http.Request) {
	var param RegistryParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		http.Error(w, "解析心跳报文失败", http.StatusBadRequest)
		return
	}
	if redisClient == nil {
		http.Error(w, "Redis 尚未初始化", http.StatusInternalServerError)
		return
	}

	// SET key 1 EX 90: 幂等刷新 TTL。节点宕机 90s 不再续期, Redis 自动过期摘除 (替代 etcd Lease)。
	// 相比"健康检查器主动探活", 省掉了额外的定时探活组件。
	key := fmt.Sprintf("%s%s:%s", registryPrefix, param.RegistryKey, param.RegistryValue)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := redisClient.Set(ctx, key, "1", heartbeatTTL).Err(); err != nil {
		fmt.Printf("心跳写入 Redis 失败: %v\n", err)
		http.Error(w, "底层 Redis 错误", http.StatusInternalServerError)
		return
	}

	// 按 XXL-Job 协议返回 200 OK
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"code": 200, "msg": null}`))
}

// GetAliveNodes (供 Router 调用) 获取某个应用下当前活着的所有节点 IP
func GetAliveNodes(appname string) []string {
	if redisClient == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// KEYS 按前缀扫出存活节点。单实例 Redis + 小规模节点足够;
	// 若日后上 Redis Cluster / 大集群, 应换成 SCAN 分批迭代, 避免阻塞。
	pattern := fmt.Sprintf("%s%s:*", registryPrefix, appname)
	keys, err := redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		fmt.Printf("查询 Redis 注册表失败: %v\n", err)
		return nil
	}

	// XXL-Job 上报的 RegistryValue 可能是 "http://10.244.0.4:9999/" (自带斜杠),
	// 这里用 TrimPrefix 原汁原味截取, 绝不能用 Split 去切。
	prefix := fmt.Sprintf("%s%s:", registryPrefix, appname)
	aliveNodes := make([]string, 0, len(keys))
	for _, key := range keys {
		ip := strings.TrimPrefix(key, prefix)
		if ip != "" {
			aliveNodes = append(aliveNodes, ip)
		}
	}
	return aliveNodes
}
