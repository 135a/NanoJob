package api

import (
	"net/http"
	"os"
	"strings"
)

// AuthMiddleware 鏍￠獙璇锋眰澶翠腑鐨?Access Token銆?// 閫氳繃鐜鍙橀噺 NANOJOB_AUTH_TOKEN 閰嶇疆 token銆?// 鑻ョ幆澧冨彉閲忔湭璁剧疆锛屽垯璺宠繃閴存潈锛堝紑鍙戞ā寮忥級銆?func AuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 鏀捐 CORS 棰勬璇锋眰
		if r.Method == http.MethodOptions {
			next(w, r)
			return
		}

		expectedToken := os.Getenv("NANOJOB_AUTH_TOKEN")
		// 鏈厤缃?token 鍒欒烦杩囬壌鏉冿紝鏂逛究鏈湴寮€鍙?		if expectedToken == "" {
			next(w, r)
			return
		}

		// 鏀寔 Authorization: Bearer <token> 鍜?X-Auth-Token 涓ょ鏂瑰紡
		authHeader := r.Header.Get("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" || token == authHeader {
			// 娌℃湁Bearer鍓嶇紑鎴朅uthorization澶翠负绌猴紝灏濊瘯X-Auth-Token
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
