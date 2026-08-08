package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// CallbackRequest Java 鎵ц鍣ㄤ笂鎶ョ殑鎵ц缁撴灉
type CallbackRequest struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`      // success / fail
	LogContent string `json:"log_content"` // 鎵ц鏃ュ織
	ExecutorIP string `json:"executor_ip"` // 鎵ц鍣ㄥ湴鍧€
}

// CallbackRecord 鎸佷箙鍖栧埌 etcd 鐨勫洖璋冭褰?type CallbackRecord struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	LogContent string `json:"log_content"`
	ExecutorIP string `json:"executor_ip"`
	Timestamp  string `json:"timestamp"`
}

// CallbackAPI 鎵ц鍥炶皟鐩稿叧鎺ュ彛
type CallbackAPI struct {
	EtcdClient *clientv3.Client
}

func (api *CallbackAPI) respondJSON(w http.ResponseWriter, code int, msg string, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Code: code, Msg: msg, Data: data})
}

// ReceiveCallback 鎺ユ敹 Java 鎵ц鍣ㄧ殑鎵ц缁撴灉鍥炶皟
// POST /api/callback
func (api *CallbackAPI) ReceiveCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	var req CallbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.respondJSON(w, 400, "JSON 瑙ｆ瀽澶辫触", nil)
		return
	}
	if req.JobID == "" {
		api.respondJSON(w, 400, "缂哄皯蹇呭～鍙傛暟 job_id", nil)
		return
	}
	if api.EtcdClient == nil {
		api.respondJSON(w, 500, "etcd 灏氭湭鍒濆鍖?, nil)
		return
	}

	now := time.Now()
	record := CallbackRecord{
		JobID:      req.JobID,
		Status:     req.Status,
		LogContent: req.LogContent,
		ExecutorIP: req.ExecutorIP,
		Timestamp:  now.Format(time.RFC3339),
	}
	data, _ := json.Marshal(record)

	// 鎸佷箙鍖栧埌 etcd: /nanojob/callback/{jobID}/{timestamp}
	key := fmt.Sprintf("/nanojob/callback/%s/%d", req.JobID, now.UnixMilli())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := api.EtcdClient.Put(ctx, key, string(data))
	if err != nil {
		api.respondJSON(w, 500, "鍥炶皟璁板綍鍐欏叆 etcd 澶辫触: "+err.Error(), nil)
		return
	}

	fmt.Printf("[Callback] 浠诲姟 %s 鎵ц缁撴灉: %s (鏉ヨ嚜 %s)\n", req.JobID, req.Status, req.ExecutorIP)
	api.respondJSON(w, 200, "鍥炶皟鎺ユ敹鎴愬姛", record)
}

// ListCallbacks 鏌ヨ鏌愪换鍔＄殑鎵ц鍘嗗彶
// GET /api/callback/list?job_id=xxx
func (api *CallbackAPI) ListCallbacks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		api.respondJSON(w, 200, "ok", nil)
		return
	}

	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		api.respondJSON(w, 400, "缂哄皯 job_id 鍙傛暟", nil)
		return
	}
	if api.EtcdClient == nil {
		api.respondJSON(w, 500, "etcd 灏氭湭鍒濆鍖?, nil)
		return
	}

	prefix := fmt.Sprintf("/nanojob/callback/%s/", jobID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := api.EtcdClient.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		api.respondJSON(w, 500, "鏌ヨ etcd 澶辫触: "+err.Error(), nil)
		return
	}

	var records []CallbackRecord
	for _, kv := range resp.Kvs {
		var rec CallbackRecord
		if err := json.Unmarshal(kv.Value, &rec); err == nil {
			records = append(records, rec)
		}
	}

	api.respondJSON(w, 200, "success", records)
}

// RegisterRoutes 娉ㄥ唽鍥炶皟鐩稿叧璺敱
func (api *CallbackAPI) RegisterRoutes() {
	http.HandleFunc("/api/callback", AuthMiddleware(api.ReceiveCallback))
	http.HandleFunc("/api/callback/list", AuthMiddleware(api.ListCallbacks))
}
