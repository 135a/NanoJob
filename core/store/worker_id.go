package store

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ClaimWorkerID 尝试从 etcd 中抢占一个范围在 1~1023 之间的空闲 WorkerID
// 结合 etcd 事务 (Txn) 和 租约 (Lease) 实现大厂级的自动发号机制
func ClaimWorkerID(cli *clientv3.Client, appName string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. 创建一个 10 秒的租约 (如果机器挂了，10秒后 etcd 会自动回收这个号码)
	grantResp, err := cli.Grant(ctx, 10)
	if err != nil {
		return 0, fmt.Errorf("创建 etcd 租约失败: %v", err)
	}

	// 2. 从 1 循环到 1023，寻找空闲的机器 ID (Snowflake 的 workerID 最大为 1023)
	for i := 1; i <= 1023; i++ {
		key := fmt.Sprintf("/nanojob/worker_ids/%s/%d", appName, i)

		// 3. 使用 etcd 事务 (Txn) 保证极高并发下的唯一抢占 (CAS 原子操作)
		// 如果 key 不存在 (CreateRevision == 0)，则 Put 进去，并绑定租约
		txn := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, time.Now().String(), clientv3.WithLease(grantResp.ID)))

		txnResp, err := txn.Commit()
		if err != nil {
			continue // 发生错误，继续尝试下一个
		}

		if txnResp.Succeeded {
			// 抢占成功！
			// 4. 开启后台协程，永久为这个租约自动续期 (KeepAlive)
			go keepWorkerIDAlive(cli, grantResp.ID)
			return int64(i), nil
		}
	}

	return 0, fmt.Errorf("机器工号池已满 (超过1023台机器)，无法分配 Worker ID")
}

// keepWorkerIDAlive 在后台通过心跳永久保活租约
func keepWorkerIDAlive(cli *clientv3.Client, leaseID clientv3.LeaseID) {
	ch, err := cli.KeepAlive(context.Background(), leaseID)
	if err != nil {
		fmt.Printf("⚠️ Worker ID 自动续期启动失败: %v\n", err)
		return
	}
	for {
		ka := <-ch
		if ka == nil {
			fmt.Printf("💥 致命警告: Worker ID 租约已失效！与 etcd 断开连接，当前机器生成的 ID 可能会冲突！\n")
			// 在严谨的生产环境中，这里应该直接 os.Exit(1) 强制退出，让 K8s 重启该机器重新分配。
			return
		}
	}
}
