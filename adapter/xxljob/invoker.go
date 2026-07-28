package xxljob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RunReq 完美复刻 XXL-Job 触发任务时的 JSON 请求体格式
type RunReq struct {
	// --- 【核心必传参数】当前代码直接依赖的 ---
	JobID           int    `json:"jobId"`           // 任务 ID (必须是数字，Java端依赖此记录日志)
	ExecutorHandler string `json:"executorHandler"` // 核心暗号：Java 端的 @XxlJob("名字")
	GlueType        string `json:"glueType"`        // 运行模式 (普通任务通常固定写死为 "BEAN")
	BroadcastIndex  int    `json:"broadcastIndex"`  // 分片序号 (海量数据分片广播策略的核心)
	BroadcastTotal  int    `json:"broadcastTotal"`  // 分片总数

	// --- 【进阶预留参数】未来完善架构功能时会用到 ---
	ExecutorParams        string `json:"executorParams"`        // 任务参数 (动态传参用，例如 "清理30天前的日志")
	ExecutorBlockStrategy string `json:"executorBlockStrategy"` // 阻塞处理策略 (如 SERIAL_EXECUTION 单机串行排队)
	ExecutorTimeout       int    `json:"executorTimeout"`       // 任务超时时间
	LogID                 int64  `json:"logId"`                 // 调度日志 ID (在后台查看执行日志详情时使用)
	LogDateTime           int64  `json:"logDateTime"`           // 调度时间

	// --- 【边缘无用参数】仅为兼容 XXL-Job 底层协议凑数，基本用不到 ---
	GlueSource     string `json:"glueSource"`     // 动态源码 (供 GLUE 模式在线动态下发代码片段使用)
	GlueUpdatetime int64  `json:"glueUpdatetime"` // 源码更新时间
}

// RunResp XXL-Job 执行器返回的标准响应格式
type RunResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// 借用 Go 的标准 HTTP Client，配置好超时时间防止被 Java 拖死
var httpClient = &http.Client{
	Timeout: 5 * time.Second, // 如果 5 秒 Java 没接请求，直接熔断放弃
}

// Trigger 向远程 Java 执行器发送触发指令
func Trigger(targetIP string, req *RunReq) error {
	// 1. 组装目标 URL (XXL-Job 执行器默认的触发路径是 /run)
	// 注意：Java 注册过来的 targetIP 实际上是完整地址 (例: http://10.0.0.1:9999/)
	targetURL := targetIP
	if !strings.HasSuffix(targetURL, "/") {
		targetURL += "/"
	}
	targetURL += "run"

	// 2. 将我们的结构体转化为 JSON 字节流
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("json 序列化失败: %v", err)
	}

	// 3. 发送 HTTP POST 请求
	resp, err := httpClient.Post(targetURL, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		return fmt.Errorf("网络请求发送失败: %v", err)
	}
	defer resp.Body.Close()

	// 4. 解析 Java 端的返回结果
	var runResp RunResp
	if err := json.NewDecoder(resp.Body).Decode(&runResp); err != nil {
		return fmt.Errorf("解析 Java 端响应失败: %v", err)
	}

	// XXL-Job 协议中，200 代表任务接收成功
	if runResp.Code != 200 {
		return fmt.Errorf("Java 端拒绝了任务, 错误信息: %s", runResp.Msg)
	}

	return nil
}
