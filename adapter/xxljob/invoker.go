package xxljob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RunReq XXL-Job 触发任务的 JSON 请求体格式
type RunReq struct {
	JobID           int    `json:"jobId"`           // 任务 ID
	ExecutorHandler string `json:"executorHandler"` // Java 端 @XxlJob("名字")
	GlueType        string `json:"glueType"`        // 运行模式, 普通任务固定 "BEAN"
	BroadcastIndex  int    `json:"broadcastIndex"`  // 分片序号
	BroadcastTotal  int    `json:"broadcastTotal"`  // 分片总数

	// 预留/兼容字段
	ExecutorParams        string `json:"executorParams"`        // 动态任务参数
	ExecutorBlockStrategy string `json:"executorBlockStrategy"` // 阻塞处理策略 (如 SERIAL_EXECUTION)
	ExecutorTimeout       int    `json:"executorTimeout"`       // 任务超时时间
	LogID                 int64  `json:"logId"`                 // 调度日志 ID
	LogDateTime           int64  `json:"logDateTime"`           // 调度时间

	// GLUE 在线模式下才用到
	GlueSource     string `json:"glueSource"`     // 动态下发的源码片段
	GlueUpdatetime int64  `json:"glueUpdatetime"` // 源码更新时间
}

// RunResp XXL-Job 执行器返回的标准响应格式
type RunResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// 借用 Go 标准 HTTP Client, 设超时防止被 Java 拖死
var httpClient = &http.Client{
	Timeout: 5 * time.Second, // 5 秒未响应则熔断
}

// Trigger 向远程 Java 执行器发送触发指令
func Trigger(targetIP string, req *RunReq) error {
	// 目标是完整地址 (如 http://10.0.0.1:9999/), 拼上 XXL-Job 默认触发路径 /run
	targetURL := targetIP
	if !strings.HasSuffix(targetURL, "/") {
		targetURL += "/"
	}
	targetURL += "run"

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("json 序列化失败: %v", err)
	}

	resp, err := httpClient.Post(targetURL, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		return fmt.Errorf("网络请求发送失败: %v", err)
	}
	defer resp.Body.Close()

	var runResp RunResp
	if err := json.NewDecoder(resp.Body).Decode(&runResp); err != nil {
		return fmt.Errorf("解析 Java 端响应失败: %v", err)
	}

	// XXL-Job 协议: 200 表示任务接收成功
	if runResp.Code != 200 {
		return fmt.Errorf("Java 端拒绝了任务, 错误信息: %s", runResp.Msg)
	}

	return nil
}
