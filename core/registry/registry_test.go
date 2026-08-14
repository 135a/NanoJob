package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestRegistryHeartbeat 基于 Redis TTL 的注册中心测试。
// 前置条件: 本地 Redis (127.0.0.1:6379); 未启动时自动 Skip。
func TestRegistryHeartbeat(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("跳过: 本地 Redis 不可用 (%v)", err)
	}
	Init(client)
	defer client.Close()

	// 1. 模拟 Java 节点上报心跳
	param := RegistryParam{
		RegistryGroup: "EXECUTOR",
		RegistryKey:   "loan-service",
		RegistryValue: "192.168.1.100:9999",
	}
	body, _ := json.Marshal(param)
	req := httptest.NewRequest(http.MethodPost, "/api/registry", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	ReceiveHeartbeat(w, req)

	// 2. XXL-Job 协议要求 200
	if w.Code != 200 {
		t.Fatalf("注册失败, 期望 HTTP 200, 实际 %d", w.Code)
	}

	// 3. 节点被判为存活
	nodes := GetAliveNodes("loan-service")
	if len(nodes) != 1 || nodes[0] != "192.168.1.100:9999" {
		t.Fatalf("名单里没有该节点, 当前名单: %v", nodes)
	}

	// 4. 注册键绑定了 TTL (宕机后 Redis 自动过期摘除)
	key := "nanojob:registry:loan-service:192.168.1.100:9999"
	ttl, err := client.TTL(context.Background(), key).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("注册键未绑定 TTL, 宕机自动清理机制失效: %v", err)
	}
	t.Logf("注册键 TTL: %s", ttl)

	// 5. 模拟节点断电: 删除键 ≈ 90s 未续租被 Redis 清理
	if err := client.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("删除失败: %v", err)
	}
	if dead := GetAliveNodes("loan-service"); len(dead) != 0 {
		t.Fatalf("节点未被剔除, 当前名单: %v", dead)
	}
}
