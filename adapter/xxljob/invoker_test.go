package xxljob

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTrigger(t *testing.T) {
	// 1. 启动一个伪装的 "Java XXL-Job 执行器" 服务器
	mockJavaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 校验请求的路径是不是 /run
		if r.URL.Path != "/run" {
			t.Errorf("期望请求路径为 /run, 实际为 %s", r.URL.Path)
		}

		// 读取 Go 引擎发过来的 JSON 报文
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		
		t.Logf("[伪装的 Java 服务器] 收到了 Go 引擎的任务分配命令: \n%s\n", string(body))

		// 模拟 Java 端成功接收任务，返回 200
		respJSON := `{"code": 200, "msg": "OK"}`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(respJSON))
	}))
	defer mockJavaServer.Close()

	// 注意：mockJavaServer.URL 类似 "http://127.0.0.1:54321"，
	// 但我们的 Trigger 函数内部会自动加 "http://" 和 "/run"，
	// 所以我们需要把 "http://" 切掉，只传入 "127.0.0.1:54321" 这个格式的 targetIP
	targetIP := strings.TrimPrefix(mockJavaServer.URL, "http://")

	// 2. 组装要发送的测试任务
	req := &RunReq{
		JobID:                 10086,
		ExecutorHandler:       "loanCreditJob", // 模拟触发计息任务
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		ExecutorTimeout:       0,
		LogID:                 999,
		LogDateTime:           time.Now().UnixMilli(),
		GlueType:              "BEAN",
		BroadcastIndex:        0, // 核心：分片 0
		BroadcastTotal:        3, // 核心：总共 3 片
	}

	t.Logf("[Go 调度引擎] 准备向 %s 发送触发指令...", targetIP)

	// 3. 执行我们的 Trigger 函数，看能不能成功把数据发过去
	err := Trigger(targetIP, req)
	if err != nil {
		t.Fatalf("Trigger 测试失败: %v", err)
	}

	t.Log("[Go 调度引擎] 任务触发成功！完美收到了 Java 端的 200 回执。")
}
