package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestRegistryHeartbeat 基于 etcd 实现的注册中心测试
// 前置条件：本地需运行 etcd (127.0.0.1:2379)，可用 docker compose up -d etcd
func TestRegistryHeartbeat(t *testing.T) {
	// 1. 连接本地 etcd 并注入客户端
	endpoints := []string{"127.0.0.1:2379"}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("连接 etcd 失败，请确认 etcd 已启动: %v", err)
	}
	defer client.Close()
	Init(client)

	// 2. 组装一个伪造的 Java 节点注册 JSON 报文并模拟心跳
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

	// 3. 验证 HTTP 响应是 XXL-Job 期待的 200
	if w.Code != 200 {
		t.Fatalf("注册失败，期望 HTTP 200, 实际收到: %d", w.Code)
	}

	// 4. 验证节点已写入 etcd 且被判为存活
	nodes := GetAliveNodes("loan-service")
	if len(nodes) != 1 || nodes[0] != "192.168.1.100:9999" {
		t.Fatalf("名单里没有找到该节点，当前名单: %v", nodes)
	}
	t.Logf("[测试通过] 心跳写入成功，当前存活节点: %v", nodes)

	// 5. 验证注册键确实绑定了租约（机器宕机后 etcd 会按 TTL 自动清理）
	key := "/nanojob/registry/loan-service/192.168.1.100:9999"
	kv, err := client.Get(context.Background(), key)
	if err != nil || len(kv.Kvs) == 0 {
		t.Fatalf("未能从 etcd 读回注册键: %v", err)
	}
	leaseID := kv.Kvs[0].Lease
	if leaseID == 0 {
		t.Fatalf("注册键未绑定租约，宕机自动清理机制失效")
	}
	t.Logf("注册键已绑定租约 ID: %d", leaseID)

	// 6. 模拟机器断电：直接撤销租约，等价于 90s 未续租被 etcd 清理
	if _, err := client.Revoke(context.Background(), clientv3.LeaseID(leaseID)); err != nil {
		t.Fatalf("撤销租约失败: %v", err)
	}

	deadNodes := GetAliveNodes("loan-service")
	if len(deadNodes) != 0 {
		t.Fatalf("节点未被剔除，当前名单: %v", deadNodes)
	}
	t.Log("[测试通过] 租约撤销后节点自动剔除，宕机清理机制生效")
}
