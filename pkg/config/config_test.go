package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conf.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// clearEnv 清掉可能残留的环境变量, 避免测试互相干扰 / 被宿主环境污染。
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"NANOJOB_DSN", "NANOJOB_REDIS_ADDR", "NANOJOB_ADVERTISE_ADDR"} {
		t.Setenv(k, "")
	}
}

func TestLoadReadsJSON(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, `{
		"cluster_name": "test-cluster",
		"mysql": {"dsn": "root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4"},
		"redis": {"addr": "127.0.0.1:6379", "password": "pw", "db": 2},
		"api_server": {"http": {"host": "0.0.0.0", "port": "8080", "advertise_addr": "http://127.0.0.1:9090"}}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 报错: %v", err)
	}
	if cfg.ClusterName != "test-cluster" {
		t.Errorf("ClusterName=%q", cfg.ClusterName)
	}
	if cfg.MySQL.DSN != "root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4" {
		t.Errorf("DSN=%q", cfg.MySQL.DSN)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Redis.Password != "pw" || cfg.Redis.DB != 2 {
		t.Errorf("Redis 配置解析错误: %+v", cfg.Redis)
	}
	if cfg.APIServer.HTTP.Port != "8080" || cfg.APIServer.HTTP.AdvertiseAddr != "http://127.0.0.1:9090" {
		t.Errorf("APIServer 配置解析错误: %+v", cfg.APIServer.HTTP)
	}
}

func TestLoadMissingFile(t *testing.T) {
	clearEnv(t)
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("缺失配置文件应报错")
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	clearEnv(t)
	if _, err := Load(writeConfig(t, `{not-json`)); err == nil {
		t.Fatal("非法 JSON 应报错")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("NANOJOB_DSN", "override-dsn")
	t.Setenv("NANOJOB_REDIS_ADDR", "override-redis")
	t.Setenv("NANOJOB_ADVERTISE_ADDR", "http://override:9999")
	path := writeConfig(t, `{
		"mysql": {"dsn": "base-dsn"},
		"redis": {"addr": "base-redis"},
		"api_server": {"http": {"advertise_addr": "http://base:8888"}}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 报错: %v", err)
	}
	if cfg.MySQL.DSN != "override-dsn" {
		t.Errorf("DSN 应被环境变量覆盖, 实际 %q", cfg.MySQL.DSN)
	}
	if cfg.Redis.Addr != "override-redis" {
		t.Errorf("Redis.Addr 应被环境变量覆盖, 实际 %q", cfg.Redis.Addr)
	}
	if cfg.APIServer.HTTP.AdvertiseAddr != "http://override:9999" {
		t.Errorf("AdvertiseAddr 应被环境变量覆盖, 实际 %q", cfg.APIServer.HTTP.AdvertiseAddr)
	}
}

// TestLoadEmptyEnvDoesNotOverride 环境变量为空串时不覆盖配置文件里的值。
func TestLoadEmptyEnvDoesNotOverride(t *testing.T) {
	t.Setenv("NANOJOB_DSN", "")
	t.Setenv("NANOJOB_REDIS_ADDR", "")
	t.Setenv("NANOJOB_ADVERTISE_ADDR", "")
	path := writeConfig(t, `{
		"mysql": {"dsn": "base-dsn"},
		"redis": {"addr": "base-redis"},
		"api_server": {"http": {"advertise_addr": "http://base:8888"}}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 报错: %v", err)
	}
	if cfg.MySQL.DSN != "base-dsn" || cfg.Redis.Addr != "base-redis" || cfg.APIServer.HTTP.AdvertiseAddr != "http://base:8888" {
		t.Fatalf("空环境变量不应覆盖配置: %+v / %+v / %+v",
			cfg.MySQL.DSN, cfg.Redis.Addr, cfg.APIServer.HTTP.AdvertiseAddr)
	}
}

func TestLoadDefaultClusterName(t *testing.T) {
	clearEnv(t)
	cfg, err := Load(writeConfig(t, `{}`))
	if err != nil {
		t.Fatalf("Load 报错: %v", err)
	}
	if cfg.ClusterName != "nanojob" {
		t.Fatalf("未配置 cluster_name 时默认应为 nanojob, 实际 %q", cfg.ClusterName)
	}
}
