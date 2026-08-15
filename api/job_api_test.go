package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nanojob/core/election"
	"nanojob/core/parser"
	"nanojob/core/scheduler"
	"nanojob/core/store"
)

const testElectionKey = "nanojob:election:api-test"

// newTestElection 起一个指向 miniredis 的选举器 (测试不依赖真实 Redis)。
func newTestElection(t *testing.T, srv *miniredis.Miniredis, nodeID string) *election.Election {
	t.Helper()
	return election.New(redis.NewClient(&redis.Options{Addr: srv.Addr()}),
		testElectionKey, nodeID, 1*time.Second)
}

// setLockValue 直接用独立 client 往 miniredis 写锁值, 模拟"锁被某节点持有"。
// (election 的 redis/key 字段私有, 测试不能碰; 锁值即 Leader 的对外地址。)
func setLockValue(t *testing.T, srv *miniredis.Miniredis, value string) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer client.Close()
	if err := client.Set(context.Background(), testElectionKey, value, 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
}

// becomeLeader 通过公开的 LoopInElect 让选举器抢锁上位 (acquire 是私有方法, 只能走循环驱动)。
// 返回时 IsMaster()==true; ttl/3 的 tick 持续续期, 保证测试期间锁不丢。
func becomeLeader(t *testing.T, e *election.Election) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 8)
	go e.LoopInElect(ctx, errCh)
	t.Cleanup(func() {
		cancel()
		e.StopElect()
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.IsMaster() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("LoopInElect 启动后未能在超时内成为 Leader")
}

func postJSON(url, body string) (*http.Request, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	return req, httptest.NewRecorder()
}

// assertAPICode 断言 respondJSON 的响应契约: HTTP 层恒为 200, 业务码放在 JSON 的 code 字段。
// (respondJSON 不调 WriteHeader —— 错误/成功一律 200, 前端按 JSON code 分支。)
func assertAPICode(t *testing.T, rec *httptest.ResponseRecorder, wantCode int) APIResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("respondJSON 的 HTTP 层恒为 200, 实际 %d", rec.Code)
	}
	var out APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应解析失败: %v (body=%s)", err, rec.Body.String())
	}
	if out.Code != wantCode {
		t.Fatalf("JSON code = %d, 期望 %d (msg=%s)", out.Code, wantCode, out.Msg)
	}
	return out
}

// ---- 读接口 (只需 Store) ----

func TestListJobsSuccess(t *testing.T) {
	ms := newMockStore()
	ms.jobs = []*store.JobInfo{
		{ID: 1, Cron: "0/10 * * * * ?", ExecutorHandler: "h1", AppName: "app"},
		{ID: 2, Cron: "0/5 * * * * ?", ExecutorHandler: "h2", AppName: "app"},
	}
	api := &JobAPI{Store: ms}

	rec := httptest.NewRecorder()
	api.ListJobs(rec, httptest.NewRequest(http.MethodGet, "/api/job/list", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d, 应 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("应带 CORS 头, 实际 %q", got)
	}
	var out struct {
		Code int              `json:"code"`
		Data []*store.JobInfo `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应解析失败: %v", err)
	}
	if out.Code != 200 || len(out.Data) != 2 {
		t.Fatalf("code=%d data长度=%d", out.Code, len(out.Data))
	}
}

func TestListJobsError(t *testing.T) {
	ms := newMockStore()
	ms.listErr = context.DeadlineExceeded
	api := &JobAPI{Store: ms}

	rec := httptest.NewRecorder()
	api.ListJobs(rec, httptest.NewRequest(http.MethodGet, "/api/job/list", nil))

	assertAPICode(t, rec, 500) // JSON code 500 (HTTP 恒 200)
}

func TestListJobsOptions(t *testing.T) {
	api := &JobAPI{} // CORS 预检不碰 Store
	rec := httptest.NewRecorder()
	api.ListJobs(rec, httptest.NewRequest(http.MethodOptions, "/api/job/list", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("OPTIONS 预检应 200, 实际 %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("预检应带 CORS 头, 实际 %q", got)
	}
}

func TestLogsSuccess(t *testing.T) {
	ms := newMockStore()
	ms.logs = []*store.JobLog{{ID: 9, JobID: 1, HandleCode: 200, HandleMsg: "ok"}}
	api := &JobAPI{Store: ms}

	rec := httptest.NewRecorder()
	api.Logs(rec, httptest.NewRequest(http.MethodGet, "/api/job/logs?id=1", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var out struct {
		Code int            `json:"code"`
		Data []*store.JobLog `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应解析失败: %v", err)
	}
	if out.Code != 200 || len(out.Data) != 1 {
		t.Fatalf("code=%d data长度=%d", out.Code, len(out.Data))
	}
}

// ---- 写收敛 Leader: Standby 的写请求 ----

// TestAddJobStandbyNoLeader 非 Leader 且无 Leader 可指 → 503 让前端稍后重试。
func TestAddJobStandbyNoLeader(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090") // 未抢锁 → Standby, 锁 key 未设
	api := &JobAPI{Election: e}

	req, rec := postJSON("/api/job/add", `{"cron":"0 0 * * * *","executorHandler":"h","appName":"app"}`)
	api.AddJob(rec, req)

	assertAPICode(t, rec, 503) // 无 Leader → JSON code 503
}

// TestAddJobStandbyRedirects 非 Leader 但 Leader 存在 → 307 重定向到 Leader, Location 带完整路径。
func TestAddJobStandbyRedirects(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	setLockValue(t, srv, "http://127.0.0.1:9091") // 锁属于别的节点
	api := &JobAPI{Election: e}

	req, rec := postJSON("/api/job/add", `{"cron":"0 0 * * * *","executorHandler":"h","appName":"app"}`)
	api.AddJob(rec, req)

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("应 307, 实际 %d", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "http://127.0.0.1:9091/api/job/add" {
		t.Fatalf("Location = %q", got)
	}
}

// TestAddJobStandbyRedirectTrimsSlash Leader 地址带尾斜杠时 (xxl-job 上报值常见) 应被 TrimRight。
func TestAddJobStandbyRedirectTrimsSlash(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	setLockValue(t, srv, "http://127.0.0.1:9091/")
	api := &JobAPI{Election: e}

	req, rec := postJSON("/api/job/add", `{"cron":"0 0 * * * *","executorHandler":"h","appName":"app"}`)
	api.AddJob(rec, req)

	if got := rec.Header().Get("Location"); got != "http://127.0.0.1:9091/api/job/add" {
		t.Fatalf("尾斜杠应被去掉, Location = %q", got)
	}
}

// ---- Leader 收到的写请求: 校验与写前强校验 ----

func TestAddJobLeaderBadJSON(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	becomeLeader(t, e)
	api := &JobAPI{Election: e} // 走到解析就 400, 不碰 Store/Scheduler

	req, rec := postJSON("/api/job/add", `{not-json`)
	api.AddJob(rec, req)

	assertAPICode(t, rec, 400) // JSON code 400 (HTTP 恒 200)
}

func TestAddJobLeaderMissingFields(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	becomeLeader(t, e)
	api := &JobAPI{Election: e}

	// 缺 Cron
	req, rec := postJSON("/api/job/add", `{"executorHandler":"h","appName":"app"}`)
	api.AddJob(rec, req)
	assertAPICode(t, rec, 400)

	// 缺 ExecutorHandler
	req, rec = postJSON("/api/job/add", `{"cron":"0 0 * * * *","appName":"app"}`)
	api.AddJob(rec, req)
	assertAPICode(t, rec, 400)
}

// TestAddJobLeadershipLost 写前强校验: IsMaster 通过后锁被抢走 (VerifyLeadership 再问 Redis) → 503。
func TestAddJobLeadershipLost(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	becomeLeader(t, e)
	// 锁在这一瞬间被新主接管 (值变了, 但本节点 IsMaster 标志还没来得及反应)
	setLockValue(t, srv, "http://127.0.0.1:9092")
	api := &JobAPI{Election: e}

	req, rec := postJSON("/api/job/add", `{"cron":"0 0 * * * *","executorHandler":"h","appName":"app"}`)
	api.AddJob(rec, req)

	assertAPICode(t, rec, 503) // 写前校验发现锁已丢 → JSON code 503
}

// TestAddJobHappyPath Leader 收下合法任务: 落库拿自增 ID + 挂时间轮 → 200 返回 id。
func TestAddJobHappyPath(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	e := newTestElection(t, srv, "http://127.0.0.1:9090")
	becomeLeader(t, e)

	ms := newMockStore()
	s := scheduler.New(ms, parser.NewCronParser(), 50*time.Millisecond, 10)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("scheduler.Start 失败: %v", err)
	}
	defer s.Stop()

	api := &JobAPI{Store: ms, Scheduler: s, Election: e}

	req, rec := postJSON("/api/job/add", `{"cron":"0 0 * * * *","executorHandler":"demoJobHandler","appName":"demo"}`)
	api.AddJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("合法写请求应 200, 实际 %d (body=%s)", rec.Code, rec.Body.String())
	}
	var out struct {
		Code int              `json:"code"`
		Data map[string]int64 `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应解析失败: %v", err)
	}
	if out.Code != 200 || out.Data["id"] != 1 {
		t.Fatalf("code=%d id=%v", out.Code, out.Data["id"])
	}
	if ms.createdCount() != 1 {
		t.Fatalf("应恰好落库一次, 实际 %d", ms.createdCount())
	}
}
