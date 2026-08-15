package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCallbackHandleSuccess 端到端回调: xxl-job-core 上报的结果数组逐条幂等回填日志, 返回 200。
// ⚠️ 特意用错拼的 logDateTim (xxl-job-core 的真实拼写) —— 解析必须成功, 这是该接口的兼容性核心。
func TestCallbackHandleSuccess(t *testing.T) {
	ms := newMockStore()
	api := &CallbackAPI{Store: ms}

	body := `[{"logId":123,"logDateTim":1700000000000,"handleCode":200,"handleMsg":"执行成功"},
	         {"logId":124,"logDateTim":0,"handleCode":500,"handleMsg":"执行失败"}]`
	req := httptest.NewRequest(http.MethodPost, "/api/callback", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	api.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d, 应 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"code":200,"msg":null}` {
		t.Fatalf("响应体 = %s", got)
	}
	// 两条结果都应逐条回填
	if len(ms.updates) != 2 {
		t.Fatalf("应回填 2 条, 实际 %d", len(ms.updates))
	}
	if u := ms.updates[0]; u.logID != 123 || u.code != 200 || u.msg != "执行成功" {
		t.Fatalf("第 1 条回填错误: %+v", u)
	}
	if u := ms.updates[1]; u.logID != 124 || u.code != 500 || u.msg != "执行失败" {
		t.Fatalf("第 2 条回填错误: %+v", u)
	}
}

// TestCallbackDecodesWrongSpellingField 单独强调: logDateTim (xxl-job 的错拼) 必须能被解析进字段。
func TestCallbackDecodesWrongSpellingField(t *testing.T) {
	ms := newMockStore()
	api := &CallbackAPI{Store: ms}

	req := httptest.NewRequest(http.MethodPost, "/api/callback",
		bytes.NewBufferString(`[{"logId":7,"logDateTim":1700000000000,"handleCode":200,"handleMsg":"ok"}]`))
	rec := httptest.NewRecorder()
	api.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	if len(ms.updates) != 1 {
		t.Fatalf("logDateTim 拼写未被正确解析, 应回填 1 条, 实际 %d", len(ms.updates))
	}
}

func TestCallbackInvalidJSON(t *testing.T) {
	api := &CallbackAPI{} // Store nil 也 OK —— 解析先失败, 走不到持久化
	req := httptest.NewRequest(http.MethodPost, "/api/callback", bytes.NewBufferString(`not-json`))
	rec := httptest.NewRecorder()

	api.Handle(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("非法 JSON 应 400, 实际 %d", rec.Code)
	}
}

func TestCallbackStoreError(t *testing.T) {
	ms := newMockStore()
	ms.updateErr = errors.New("mysql down")
	api := &CallbackAPI{Store: ms}

	req := httptest.NewRequest(http.MethodPost, "/api/callback",
		bytes.NewBufferString(`[{"logId":1,"handleCode":200,"handleMsg":"x"}]`))
	rec := httptest.NewRecorder()

	api.Handle(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("落库失败应 500, 实际 %d", rec.Code)
	}
}
