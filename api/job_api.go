package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"nanojob/core/store"
	"nanojob/pkg/uid"
)

// JobAPI 灏佽浜嗗澶栨毚闇茬殑绠＄悊绔帴鍙?type JobAPI struct {
	Store          *store.EtcdStore
	// 杩欐槸涓€涓€滈挬瀛?Hook)鈥濆嚱鏁帮細褰?API 鎺ユ敹鍒版柊浠诲姟鏃讹紝涓嶄粎瑕佸瓨搴擄紝杩樿閫氳繃杩欎釜閽╁瓙閫氱煡 main.go 鐑寕杞藉埌鏃堕棿杞?	ScheduleNotify func(job *store.JobInfo) 
}

// APIResponse 缁熶竴杩斿洖缁欏墠绔?JSON 鏍煎紡瑙勮寖
type APIResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// respondJSON 鏄竴涓緟鍔╁伐鍏凤紝椤轰究瑙ｅ喅浜嗗墠绔渶澶寸柤鐨勮法鍩?(CORS) 闂
func (api *JobAPI) respondJSON(w http.ResponseWriter, code int, msg string, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")
	
	json.NewEncoder(w).Encode(APIResponse{Code: code, Msg: msg, Data: data})
}

// ======== 涓嬮潰鏄牳蹇冪殑 CRUD 鎺ュ彛 ========

// ListJobs 鎺ュ彛锛氳幏鍙栨墍鏈変换鍔?func (api *JobAPI) ListJobs(w http.ResponseWriter, r *http.Request) {
	// 閬囧埌鍓嶇鍙戣捣鐨?OPTIONS 璺ㄥ煙棰勬璇锋眰锛岀洿鎺ユ斁琛?	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobs, err := api.Store.ListJobs(ctx)
	if err != nil {
		api.respondJSON(w, 500, "浠?etcd 鑾峰彇浠诲姟澶辫触: "+err.Error(), nil)
		return
	}
	api.respondJSON(w, 200, "success", jobs)
}

// AddJob 鎺ュ彛锛氭柊寤轰换鍔″苟鐑姞杞?func (api *JobAPI) AddJob(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	var job store.JobInfo
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		api.respondJSON(w, 400, "JSON 鏍煎紡瑙ｆ瀽澶辫触", nil)
		return
	}

	// 銆愮敓浜х骇鏀归€犮€戯細鍚庣閫氳繃鍏ㄦ柊澶у巶绾у姩鎬侀厤缃ソ鐨?UID 鐢熸垚鍣紝鐢熸垚鍏ㄥ眬鍞竴 ID
	job.ID = uid.Generate()

	// 鍩虹鍙傛暟闃插憜鏍￠獙 (涓嶅啀闇€瑕佸墠绔紶 ID)
	if job.Cron == "" || job.ExecutorHandler == "" {
		api.respondJSON(w, 400, "缂哄皯蹇呭～鍙傛暟 (Cron/Handler)", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. 鐗╃悊钀藉湴锛氬啓鍏ョ湡瀹?etcd
	if err := api.Store.SaveJob(ctx, &job); err != nil {
		api.respondJSON(w, 500, "鍐欏叆 etcd 澶辫触: "+err.Error(), nil)
		return
	}

	// 2. 鍐呭瓨鐑姞杞斤細閫氱煡涓诲紩鎿庣珛鍒昏绠?Cron 鍊掕鏃讹紝涓㈣繘鏃堕棿杞紒
	if api.ScheduleNotify != nil {
		api.ScheduleNotify(&job)
	}

	api.respondJSON(w, 200, "浠诲姟鍒涘缓骞跺惎鍔ㄦ垚鍔燂紒", nil)
}

// RegisterRoutes 娉ㄥ唽鍏ㄩ儴璺敱锛堝叏閮ㄦ帴鍙ｇ粡杩?AuthMiddleware 閴存潈鎷︽埅锛?func (api *JobAPI) RegisterRoutes() {
	http.HandleFunc("/api/job/list", AuthMiddleware(api.ListJobs))
	http.HandleFunc("/api/job/add", AuthMiddleware(api.AddJob))
}
