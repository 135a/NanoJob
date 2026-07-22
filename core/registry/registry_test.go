package registry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRegistryHeartbeat(t *testing.T) {
	// 1. 组装一个伪造的 Java 节点注册 JSON 报文
	param := RegistryParam{
		RegistryGroup: "EXECUTOR",
		RegistryKey:   "loan-service", // 假设是信贷微服务
		RegistryValue: "192.168.1.100:9999",
	}
	body, _ := json.Marshal(param)

	// 2. 构造一个纯内存的 HTTP POST 请求（模拟 Java 发来的心跳）
	req := httptest.NewRequest(http.MethodPost, "/api/registry", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// 3. 调用我们的心跳接收器
	t.Log("模拟 Java 节点发起注册心跳...")
	ReceiveHeartbeat(w, req)

	// 4. 验证 HTTP 响应是不是 XXL-Job 期待的 200 OK
	if w.Code != 200 {
		t.Fatalf("注册失败，期望 HTTP 200, 实际收到: %d", w.Code)
	}
	t.Logf("Go 引擎响应成功: %s", w.Body.String())

	// 5. 验证这个 IP 是否已经成功存入了名单，并且被判定为“存活”
	nodes := GetAliveNodes("loan-service")
	if len(nodes) != 1 || nodes[0] != "192.168.1.100:9999" {
		t.Fatalf("名单里没有找到该节点，当前名单: %v", nodes)
	}
	t.Logf("[测试通过] 名单更新成功，当前存活节点: %v\n", nodes)

	// 6. 模拟机器断电宕机：人为把该节点的心跳时间拨回到 91 秒之前
	t.Log("模拟 Java 机器断电，91 秒未发送心跳...")
	registryMutex.Lock()
	globalRegistry["loan-service"].mu.Lock()
	globalRegistry["loan-service"].nodes["192.168.1.100:9999"] = time.Now().Add(-91 * time.Second)
	globalRegistry["loan-service"].mu.Unlock()
	registryMutex.Unlock()

	// 7. 再次获取存活节点，测试过滤逻辑
	deadNodes := GetAliveNodes("loan-service")
	if len(deadNodes) != 0 {
		t.Fatalf("节点超时未被剔除，当前名单: %v", deadNodes)
	}
	t.Log("[测试通过] 防御机制生效！该节点已被判定为宕机，不会再给它派发任务！")
}
