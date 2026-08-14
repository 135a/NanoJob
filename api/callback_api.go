package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"nanojob/core/store"
)

// CallbackParam XXL-Job 执行器跑完后回调上报的单条结果 (对应 xxl-job-core 的 HandleCallbackParam)。
// ⚠️ 字段名是 logDateTim (xxl-job-core 的拼写), 不是 logDateTime —— 按错名解析会全部丢失。
type CallbackParam struct {
	LogID      int64  `json:"logId"`
	LogDateTim int64  `json:"logDateTim"`
	HandleCode int    `json:"handleCode"` // 200=成功 / 500=失败
	HandleMsg  string `json:"handleMsg"`  // 执行日志内容
}

// CallbackAPI 处理执行器结果回调。
//
// 端点在**所有**节点注册、不必收敛 Leader —— 日志追加到共享 MySQL、按自增 logId 定位,
// 与"砍#2 任务配置写收敛"是两码事。Java 执行器的 xxl.job.admin.addresses 只要指向任意一台
// Go 引擎, xxl-job-core 跑完会自动 POST 到这里回填结果, 闭环达成。
type CallbackAPI struct {
	Store store.Store
}

// Handle POST /api/callback, body 为 HandleCallbackParam JSON 数组:
//
//	[{"logId":123,"logDateTim":1700000000000,"handleCode":200,"handleMsg":"..."}]
func (api *CallbackAPI) Handle(w http.ResponseWriter, r *http.Request) {
	var params []CallbackParam
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, `{"code":500,"msg":"解析回调报文失败"}`, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, p := range params {
		// 按 log_id 幂等回填: 未知 logId 影响 0 行, 不报错; 重复回调覆盖即可
		if err := api.Store.UpdateLog(ctx, p.LogID, p.HandleCode, p.HandleMsg); err != nil {
			http.Error(w, `{"code":500,"msg":"日志落库失败"}`, http.StatusInternalServerError)
			return
		}
	}

	// 按 XXL-Job 协议返回 200 OK
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"code":200,"msg":null}`))
}
