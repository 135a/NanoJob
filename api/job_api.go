package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/bwmarrin/snowflake"
	"nanojob/core/store"
)

var sfNode *snowflake.Node

func init() {
	var err error
	// 初始化雪花算法节点，这里的 1 是机器 ID。生产环境中不同机器应该配置不同的 ID。
	sfNode, err = snowflake.NewNode(1)
	if err != nil {
		panic("初始化雪花算法失败: " + err.Error())
	}
}

// JobAPI 封装了对外暴露的管理端接口
type JobAPI struct {
	Store          *store.EtcdStore
	// 这是一个“钩子(Hook)”函数：当 API 接收到新任务时，不仅要存库，还要通过这个钩子通知 main.go 热挂载到时间轮
	ScheduleNotify func(job *store.JobInfo) 
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

	jobs, err := api.Store.ListJobs(ctx)
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

	// 【生产级改造】：后端自动通过雪花算法生成全局唯一的纯数字 ID
	// 这样不仅解决了前端乱填字符串导致 XXL-Job 派发失败的问题，而且生成的 ID 是按时间趋势递增的
	job.ID = sfNode.Generate().String()

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

	// 2. 内存热加载：通知主引擎立刻计算 Cron 倒计时，丢进时间轮！
	if api.ScheduleNotify != nil {
		api.ScheduleNotify(&job)
	}

	api.respondJSON(w, 200, "任务创建并启动成功！", nil)
}

// RegisterRoutes 注册全部路由
func (api *JobAPI) RegisterRoutes() {
	http.HandleFunc("/api/job/list", api.ListJobs)
	http.HandleFunc("/api/job/add", api.AddJob)
}
