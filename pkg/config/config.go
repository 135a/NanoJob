package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config 服务配置 (参考 easytask 的 conf.json, 只留核心字段: mysql / redis / api_server / cluster_name)
type Config struct {
	ClusterName string `json:"cluster_name"`
	MySQL       struct {
		DSN string `json:"dsn"` // 如 "root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4&parseTime=true"
	} `json:"mysql"`
	Redis struct {
		Addr     string `json:"addr"`
		Password string `json:"password"`
		DB       int    `json:"db"`
	} `json:"redis"`
	APIServer struct {
		HTTP struct {
			Host          string `json:"host"`           // 监听地址, 空=全部网卡
			Port          string `json:"port"`           // HTTP 监听端口
			AdvertiseAddr string `json:"advertise_addr"` // 本节点对外地址, 选举锁持有值 + Standby 重定向目标
		} `json:"http"`
	} `json:"api_server"`
}

// Load 加载 JSON 配置, 支持环境变量覆盖关键字段 (docker-compose 多引擎时每台传不同地址):
//   - NANOJOB_DSN            覆盖 MySQL.DSN
//   - NANOJOB_REDIS_ADDR     覆盖 Redis.Addr
//   - NANOJOB_ADVERTISE_ADDR 覆盖 APIServer.HTTP.AdvertiseAddr
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 [%s] 失败: %v", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 [%s] 失败: %v", path, err)
	}

	if v := os.Getenv("NANOJOB_DSN"); v != "" {
		cfg.MySQL.DSN = v
	}
	if v := os.Getenv("NANOJOB_REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("NANOJOB_ADVERTISE_ADDR"); v != "" {
		cfg.APIServer.HTTP.AdvertiseAddr = v
	}
	if cfg.ClusterName == "" {
		cfg.ClusterName = "nanojob"
	}
	return &cfg, nil
}
