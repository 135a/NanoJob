package election

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Election 基于 Redis 的分布式主从选举。
//
// 汲取 easytask/autoelect 的教训:
//   - 抢锁用 `SET key nodeID NX EX ttl` 一步原子完成, 绝不能先 SET 再单独 EXPIRE
//     (两步之间进程崩溃会留下永不过期的锁);
//   - 续期必须带"值校验"(Lua 判断 key 当前值仍是自己才刷新 TTL)。否则旧主失联后
//     "自认为仍持锁续期" 与接管的新主同时派发 = 双主脑裂, 和 etcd 那个 LockWait
//     超时误判 true 的 bug 同源。
type Election struct {
	redis    *redis.Client
	key      string        // 锁 key, 如 nanojob:election:<cluster_name>
	nodeID   string        // 竞选者标识 = 本节点对外地址 (http://host:port), 供 Standby 重定向
	ttl      time.Duration // 锁 TTL
	isMaster atomic.Bool
	stopCh   chan struct{}
	changes  chan bool // 领导权变化通知: true=上位, false=让位
}

// renewScript 值校验式续期: 只有 key 当前值仍是自己才刷新 TTL。
// 返回 1 = 续期成功; -1 = 锁已丢 (被他人抢占或已过期)。
var renewScript = redis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('pexpire', KEYS[1], ARGV[2])
else
	return -1
end`)

// New 创建一个选举器。key 传全路径 (含 cluster 前缀), nodeID 传本节点对外地址。
func New(redisClient *redis.Client, key, nodeID string, ttl time.Duration) *Election {
	return &Election{
		redis:   redisClient,
		key:     key,
		nodeID:  nodeID,
		ttl:     ttl,
		stopCh:  make(chan struct{}),
		changes: make(chan bool, 1),
	}
}

// LoopInElect 后台运行选举循环 (阻塞), 直到 StopElect 或 ctx 取消。
// 每个 tick (ttl/3) 做一件事: 已是 Leader 则续期, 否则尝试抢锁。
func (e *Election) LoopInElect(ctx context.Context, errCh chan<- error) error {
	if errCh == nil {
		return errors.New("errCh 不能为 nil")
	}
	defer func() { e.isMaster.Store(false) }()

	ticker := time.NewTicker(e.ttl / 3)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if e.isMaster.Load() {
				if err := e.renew(); err != nil {
					errCh <- err
				}
			} else if err := e.acquire(); err != nil {
				errCh <- err
			}
		}
	}
}

// renew 续期; 锁已丢 (res==-1) 或 Redis 不可达 (err!=nil) 都降级并广播。
func (e *Election) renew() error {
	res, err := renewScript.Run(context.Background(), e.redis, []string{e.key},
		e.nodeID, int64(e.ttl/time.Millisecond)).Int()
	if err != nil {
		// Redis 断连/超时: 无法确认锁归属。可能是 Redis 宕机 (谁也连不上),
		// 也可能是网络分区 (只有本节点连不上, Standby 仍能连上并在 TTL 到期后抢锁上位)。
		// 两者无法区分, 必须保守降级 —— 否则网络分区下旧主继续派发、新主也已上位 = 双主脑裂。
		// 降级是 fail-safe: 即便真是 Redis 宕机, 恢复后节点会重新参与竞选。
		e.isMaster.Store(false)
		e.notify(false)
		return fmt.Errorf("选举锁续期失败, 已降级为 Standby: %v", err)
	}
	if res == -1 {
		// 锁已被他人拿走或已过期 —— 立即降级, 让调度协程停轮子
		e.isMaster.Store(false)
		e.notify(false)
		fmt.Printf("⚠️ [%s] 选举锁已丢失, 降级为 Standby\n", e.nodeID)
	}
	return nil
}

// acquire 尝试抢锁; 抢到则上位并广播。
func (e *Election) acquire() error {
	ok, err := e.redis.SetNX(context.Background(), e.key, e.nodeID, e.ttl).Result()
	if err != nil {
		return fmt.Errorf("选举锁抢锁失败: %v", err)
	}
	if ok {
		e.isMaster.Store(true)
		e.notify(true)
		fmt.Printf("🔥 [%s] 竞选成功, 已接管全局调度\n", e.nodeID)
	}
	return nil
}

// notify 非阻塞广播领导权变化 (缓冲 1, 事件间相隔一个 tick, 不会丢)
func (e *Election) notify(v bool) {
	select {
	case e.changes <- v:
	default:
	}
}

// Changes 领导权变化通知通道: true=上位, false=让位。由调度协程消费。
func (e *Election) Changes() <-chan bool {
	return e.changes
}

// IsMaster 当前节点是否持有领导权
func (e *Election) IsMaster() bool {
	return e.isMaster.Load()
}

// VerifyLeadership 写前强校验: 直接问 Redis 锁的当前值是否仍是本节点。
// 防住 "IsMaster() 检查通过后、真正写入 MySQL 前" 的一瞬间锁被抢走。
func (e *Election) VerifyLeadership(ctx context.Context) bool {
	val, err := e.redis.Get(ctx, e.key).Result()
	if err != nil {
		return false
	}
	return val == e.nodeID
}

// LeaderAddr 返回当前 Leader 的对外地址 (锁的持有值), 供 Standby 重定向写请求。
func (e *Election) LeaderAddr(ctx context.Context) string {
	val, err := e.redis.Get(ctx, e.key).Result()
	if err != nil {
		return ""
	}
	return val
}

// StopElect 停止选举循环
func (e *Election) StopElect() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}
