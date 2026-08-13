package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdStore 完美实现了 Store 接口，把所有的任务状态托付给 etcd
type EtcdStore struct {
	client *clientv3.Client
	prefix string // 命名空间：我们把所有任务都统一存在 /nanojob/jobs/ 目录下
}

// NewEtcdStore 建立与 etcd 集群的连接
func NewEtcdStore(endpoints []string) (*EtcdStore, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second, // 5秒连不上直接报错，绝不阻塞启动过程
	})
	if err != nil {
		return nil, fmt.Errorf("连接 etcd 集群失败: %v", err)
	}
	
	return &EtcdStore{
		client: cli,
		prefix: "/nanojob/jobs/", 
	}, nil
}

// GetClient 暴露底层的 etcd 客户端，供分布式锁(Leader Election)使用
func (s *EtcdStore) GetClient() *clientv3.Client {
	return s.client
}

// SaveJob 把任务装换成 JSON 存入 etcd
func (s *EtcdStore) SaveJob(ctx context.Context, job *JobInfo) error {
	key := s.prefix + job.ID
	
	val, err := json.Marshal(job)
	if err != nil {
		return err
	}
	
	// etcd 经典的 Put 操作
	_, err = s.client.Put(ctx, key, string(val))
	return err
}

// GetJob 根据 ID 获取任务详情
func (s *EtcdStore) GetJob(ctx context.Context, id string) (*JobInfo, error) {
	key := s.prefix + id
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	
	// 因为是强一致性，如果查不到，数组就是空的
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("未找到任务: %s", id)
	}
	
	var job JobInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
		return nil, err
	}
	
	return &job, nil
}

// ListJobs 这是 Go 引擎重启时恢复状态的核心杀手锏！
// [架构修复 1] 现在额外返回 etcd 全局 Revision：
//   Leader 用它做 read-then-watch —— 先 ListJobs(rev) 捞存量，再 WatchJobs(rev+1) 接增量，
//   封死"List 与 Watch 之间新增任务被漏掉"的竞态窗口。
func (s *EtcdStore) ListJobs(ctx context.Context) ([]*JobInfo, int64, error) {
	// 注意这里加了 clientv3.WithPrefix() 参数
	// 它的作用是扫描所有以 "/nanojob/jobs/" 开头的 key，一次性全捞出来！
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	var jobs []*JobInfo
	for _, kv := range resp.Kvs {
		var job JobInfo
		if err := json.Unmarshal(kv.Value, &job); err == nil {
			jobs = append(jobs, &job)
		}
	}

	// resp.Header.Revision 是本次读取"看到"的最新全局版本号，
	// 任何在它之后发生的写操作，版本号都会更大 —— 从 rev+1 开始 Watch 就绝不会漏事件。
	return jobs, resp.Header.Revision, nil
}

// WatchJobs 从指定 revision 开始监听任务变更（新增/修改任务）。
// [架构修复 1] 任意引擎只负责"写 etcd"，只有持有租约的 Leader 通过本方法统一消费增量：
//   无论任务打到哪台引擎，最终都会落到 /nanojob/jobs/ 下，Leader 的 Watch 一定会收到，
//   彻底解决"任务写入 Standby 变成孤儿、无人调度"的问题。
func (s *EtcdStore) WatchJobs(ctx context.Context, rev int64) clientv3.WatchChan {
	return s.client.Watch(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
}

// DeleteJob 从 etcd 中彻底抹除任务
func (s *EtcdStore) DeleteJob(ctx context.Context, id string) error {
	key := s.prefix + id
	_, err := s.client.Delete(ctx, key)
	return err
}
