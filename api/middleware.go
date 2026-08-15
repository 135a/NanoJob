package api

import (
	"net/http"
	"os"
	"strings"
)

// AuthMiddleware 校验请求头中的 Access Token。
// 通过环境变量 NANOJOB_AUTH_TOKEN 配置 token。
// 若环境变量未设置，则跳过鉴权（开发模式，向后兼容）。
func AuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 放行 CORS 预检请求
		if r.Method == http.MethodOptions {
			next(w, r)
			return
		}

		expectedToken := os.Getenv("NANOJOB_AUTH_TOKEN")
		// 未配置 token 则跳过鉴权，方便本地开发
		if expectedToken == "" {
			next(w, r)
			return
		}

		// 支持 Authorization: Bearer <token> 和 X-Auth-Token 两种方式
		authHeader := r.Header.Get("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" || token == authHeader {
			// 没有 Bearer 前缀或 Authorization 头为空，尝试 X-Auth-Token
			token = r.Header.Get("X-Auth-Token")
		}

		if token != expectedToken {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"code":401,"msg":"unauthorized: invalid or missing token","data":null}`))
			return
		}

		next(w, r)
	}
}
