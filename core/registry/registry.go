package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var etcdClient *clientv3.Client

// Init 注入 etcd 客户端
func Init(client *clientv3.Client) {
	etcdClient = client
}

// RegistryParam XXL-Job 客户端心跳上报的 JSON 请求格式
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"` // 通常是 "EXECUTOR"
	RegistryKey   string `json:"registryKey"`   // 应用名，比如 "loan-service"
	RegistryValue string `json:"registryValue"` // 节点的 IP:Port (如 192.168.1.100:9999)
}

// ReceiveHeartbeat 提供给 Java 端调用的 HTTP 接口处理函数
func ReceiveHeartbeat(w http.ResponseWriter, r *http.Request) {
	var param RegistryParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		http.Error(w, "解析心跳报文失败", http.StatusBadRequest)
		return
	}

	if etcdClient == nil {
		http.Error(w, "etcd 尚未初始化", http.StatusInternalServerError)
		return
	}

	// 核心改造：将心跳写入 etcd，并绑定 90 秒租约
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. 申请一个 90 秒的租约 (如果机器挂了，90秒后 etcd 会自动删掉它的记录)
	lease, err := etcdClient.Grant(ctx, 90)
	if err != nil {
		fmt.Printf("心跳租约申请失败: %v\n", err)
		http.Error(w, "底层 etcd 错误", http.StatusInternalServerError)
		return
	}

	// 2. 写入键值对，Key 格式为: /nanojob/registry/{appName}/{ip}
	key := fmt.Sprintf("/nanojob/registry/%s/%s", param.RegistryKey, param.RegistryValue)
	
	// 3. 把 Key 和 租约 绑定起来
	_, err = etcdClient.Put(ctx, key, "", clientv3.WithLease(lease.ID))
	if err != nil {
		fmt.Printf("心跳写入 etcd 失败: %v\n", err)
		http.Error(w, "底层 etcd 错误", http.StatusInternalServerError)
		return
	}

	// 按 XXL-Job 协议返回 200 OK
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"code": 200, "msg": null}`))
}

// GetAliveNodes （供我们的 Router 调用）获取某个应用下当前活着的所有节点 IP
func GetAliveNodes(appname string) []string {
	if etcdClient == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 查询前缀为 /nanojob/registry/{appName}/ 的所有存活节点
	prefix := fmt.Sprintf("/nanojob/registry/%s/", appname)
	resp, err := etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("查询 etcd 注册表失败: %v\n", err)
		return nil
	}

	var aliveNodes []string
	for _, kv := range resp.Kvs {
		keyStr := string(kv.Key)
		// 严谨改造：XXL-Job 上报的可能是 "http://10.244.0.4:9999/"，里面自带斜杠
		// 所以绝对不能用 strings.Split，必须用 TrimPrefix 原汁原味地截取出来！
		ip := strings.TrimPrefix(keyStr, prefix)
		if ip != "" {
			aliveNodes = append(aliveNodes, ip)
		}
	}

	return aliveNodes
}
