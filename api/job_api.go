package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"nanojob/core/store"
	"nanojob/pkg/uid"
)

// JobAPI 封装了对外暴露的管理端接口
// [架构修复 1] 原来的 ScheduleNotify 本机热加载钩子已被彻底删除：
//   它导致"任务写入 Standby → Standby 的轮子从未 Start() → 任务变孤儿"。
//   现在任意引擎（无论 Leader/Standby）都只负责把任务写进 etcd，
//   由持有租约的 Leader 通过 etcd Watch 统一消费增量去调度 —— 调度权与"谁收到请求"彻底解耦。
type JobAPI struct {
	Store *store.EtcdStore
}

// APIResponse 统一返回给前端 JSON 格式规范
type APIResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// respondJSON 是一个辅助工具，顺便解决了前端最头疼的跨域 (CORS) 问题
func (api *JobAPI) respondJSON(w http.ResponseWriter, code int, msg string, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")
	
	json.NewEncoder(w).Encode(APIResponse{Code: code, Msg: msg, Data: data})
}

// ======== 下面是核心的 CRUD 接口 ========

// ListJobs 接口：获取所有任务
func (api *JobAPI) ListJobs(w http.ResponseWriter, r *http.Request) {
	// 遇到前端发起的 OPTIONS 跨域预检请求，直接放行
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// ListJobs 现在返回 (jobs, rev, err)，查询接口用不到 rev，用 _ 忽略
	jobs, _, err := api.Store.ListJobs(ctx)
	if err != nil {
		api.respondJSON(w, 500, "从 etcd 获取任务失败: "+err.Error(), nil)
		return
	}
	api.respondJSON(w, 200, "success", jobs)
}

// AddJob 接口：新建任务并热加载
func (api *JobAPI) AddJob(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	var job store.JobInfo
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		api.respondJSON(w, 400, "JSON 格式解析失败", nil)
		return
	}

	// 【生产级改造】：后端通过全新大厂级动态配置好的 UID 生成器，生成全局唯一 ID
	job.ID = uid.Generate()

	// 基础参数防呆校验 (不再需要前端传 ID)
	if job.Cron == "" || job.ExecutorHandler == "" {
		api.respondJSON(w, 400, "缺少必填参数 (Cron/Handler)", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. 物理落地：写入真实 etcd
	if err := api.Store.SaveJob(ctx, &job); err != nil {
		api.respondJSON(w, 500, "写入 etcd 失败: "+err.Error(), nil)
		return
	}

	// 2. [架构修复 1] 写入即返回，调度交给 Leader 的 WatchJobs 统一消费。
	//    ❌ 旧实现：api.ScheduleNotify(&job) 本机热加载 —— 请求打到 Standby 时任务变孤儿。
	//    ✅ 新实现：Leader 通过 Watch 收到这次 Put 后，自动 scheduleJob 挂载进时间轮。
	api.respondJSON(w, 200, "任务创建成功！已写入 etcd，Leader 将自动接管调度。", nil)
}

// RegisterRoutes 注册全部路由
func (api *JobAPI) RegisterRoutes() {
	http.HandleFunc("/api/job/list", api.ListJobs)
	http.HandleFunc("/api/job/add", api.AddJob)
}
