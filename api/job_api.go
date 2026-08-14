package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"nanojob/core/election"
	"nanojob/core/scheduler"
	"nanojob/core/store"
)

// JobAPI 封装管理端接口。
//
// [砍#2] 写请求收敛到 Leader: 非 Leader 收到写 → 307 重定向到当前 Leader,
// 前端只需配一个地址 (任意一台)。配合 Leader 的"写前校验选举锁", 不再需要 etcd Watch。
type JobAPI struct {
	Store     store.Store
	Scheduler *scheduler.Scheduler
	Election  *election.Election
}

// APIResponse 统一返回给前端 JSON 格式规范
type APIResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// respondJSON 辅助函数, 顺便解决前端最头疼的跨域 (CORS) 问题
func (api *JobAPI) respondJSON(w http.ResponseWriter, code int, msg string, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Code: code, Msg: msg, Data: data})
}

// ListJobs 接口: 获取所有任务
func (api *JobAPI) ListJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobs, err := api.Store.ListJobs(ctx)
	if err != nil {
		api.respondJSON(w, 500, "从 MySQL 获取任务失败: "+err.Error(), nil)
		return
	}
	api.respondJSON(w, 200, "success", jobs)
}

// AddJob 接口: 新建任务 (写路径收敛 Leader)
func (api *JobAPI) AddJob(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	// 1. 写收敛 Leader: 非 Leader 直接 307 重定向到当前 Leader。
	//    不能先读 Body —— 重定向后 Body 要原样带给 Leader。
	if !api.Election.IsMaster() {
		leader := api.Election.LeaderAddr(context.Background())
		if leader == "" {
			api.respondJSON(w, 503, "当前没有 Leader, 请稍后重试", nil)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Location", strings.TrimRight(leader, "/")+"/api/job/add")
		w.WriteHeader(http.StatusTemporaryRedirect) // 307: 保留方法 + Body, fetch 自动跟随
		return
	}

	// 2. 反序列化 + 防呆校验 (ID 由 MySQL 自增, 不需要前端传)
	var job store.JobInfo
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		api.respondJSON(w, 400, "JSON 格式解析失败", nil)
		return
	}
	if job.Cron == "" || job.ExecutorHandler == "" {
		api.respondJSON(w, 400, "缺少必填参数 (Cron/Handler)", nil)
		return
	}

	// 3. 写前强校验: IsMaster 通过后、真正写入前的一瞬间, 锁可能已被抢走 —— 再问一次 Redis
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if !api.Election.VerifyLeadership(ctx) {
		api.respondJSON(w, 503, "领导权已变更, 请重试", nil)
		return
	}

	// 4. 落库 + 挂时间轮 (调度器内部保证交接期不重复挂载)
	id, err := api.Scheduler.AddJob(ctx, &job)
	if err != nil {
		api.respondJSON(w, 500, "写入 MySQL 失败: "+err.Error(), nil)
		return
	}
	api.respondJSON(w, 200, "任务创建成功, Leader 已挂载时间轮", map[string]int64{"id": id})
}

// Logs 接口: 查某个任务最近的执行日志 (回调闭环的展示侧)
func (api *JobAPI) Logs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	jobID, _ := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	logs, err := api.Store.ListLogs(ctx, jobID)
	if err != nil {
		api.respondJSON(w, 500, "查询日志失败: "+err.Error(), nil)
		return
	}
	api.respondJSON(w, 200, "success", logs)
}

// RegisterRoutes 注册全部路由
func (api *JobAPI) RegisterRoutes() {
	http.HandleFunc("/api/job/list", api.ListJobs)
	http.HandleFunc("/api/job/add", api.AddJob)
	http.HandleFunc("/api/job/logs", api.Logs)
}
