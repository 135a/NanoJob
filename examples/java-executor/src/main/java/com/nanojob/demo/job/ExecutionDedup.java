package com.nanojob.demo.job;

import com.xxl.job.core.context.XxlJobHelper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * [架构修复 3] 执行器幂等去重 —— at-least-once 投递的兜底伞。
 *
 * ─────────────────────────────────────────────────────────────────
 * 背景：为什么调度端拼命去重，执行端还是可能收到重复请求？
 * ─────────────────────────────────────────────────────────────────
 * 本调度系统对外契约是 at-least-once（至少一次）投递：
 *   1. 旧 Leader 合法在位时，对 slot N 正常派发过一次；
 *   2. 随后旧 Leader 与 etcd 失联，等它被新 Leader 取代时，
 *      新 Leader 无法确认"旧主到底派没派"，只能把它当成漏发再补一次。
 * 于是同一触发点（slot N）可能被派发两次。这是分布式系统的结构性问题，
 * 调度层无论怎么防（Watch 统一消费 + fail-fast），都根除不掉这个交接窗口的重复。
 *
 * ─────────────────────────────────────────────────────────────────
 * 解法：确定性执行 ID + 原子占位
 * ─────────────────────────────────────────────────────────────────
 * 1. 调度端（Go 引擎）为每一次触发生成【确定性】执行 ID = 任务ID + 触发时间戳，
 *    例如 "1834567890123456789:1723456789"。
 *    注意必须是确定性派生（不是随机 UUID）：
 *    —— 旧主派发 slot N、新主 misfire 补偿 slot N，算出来的是同一个 ID；
 *    —— 若用随机 UUID，两次派发 ID 不同，执行器就认不出重复，去重失效。
 * 2. 执行端（本类）收到请求后，先按执行 ID 做【原子占位】；
 *    已占位过 = 重复派发 → 直接跳过，不执行业务。
 *
 * 占位必须原子（本 demo 用 ConcurrentHashMap.putIfAbsent），
 * 绝不能"先 SELECT 再 INSERT"——否则两个并发请求会同时通过检查、双双执行（TOCTOU 竞态）。
 *
 * ─────────────────────────────────────────────────────────────────
 * ⚠️ 生产升级建议（多实例 / 多机房时必做）：
 * ─────────────────────────────────────────────────────────────────
 * 本 demo 的内存表只在【单个 JVM 进程内】去重。一旦执行器水平扩容到多个实例，
 * 两次重复派发可能打到不同实例，各自的内存表互不相通 → 去重失效。
 * 生产环境必须把占位记录放进【共享存储】，三选一：
 *   1) MySQL：执行日志表加 exec_id 唯一索引，INSERT IGNORE 影响行数=0 即重复；
 *   2) Redis：SETNX nanojob:exec:<executionId>，返回 1 才执行，天然支持 TTL；
 *   3) 其他任何支持"原子条件写入"的存储（ZooKeeper / etcd 的 Txn 等）。
 * 占位记录保留时间：本系统 misfire 宽限期仅 5 秒，保留几分钟足够；
 * 若业务 misfire 窗口很大，按窗口长度设置 TTL 即可（Redis 天然支持）。
 */
public final class ExecutionDedup {

    /** 已占位的执行 ID 集合。demo 用进程内内存表；生产请替换为 DB 唯一索引 / Redis SETNX。 */
    private static final Map<String, Boolean> CLAIMED = new ConcurrentHashMap<>();

    private ExecutionDedup() {
    }

    /**
     * 尝试原子占位（在业务代码的最前面调用一次）。
     *
     * @return true  = 占位成功，本次触发是第一次到达，请正常执行业务；
     *         false = 占位失败，同一执行 ID 已处理过，请直接 return 跳过（重复派发）。
     */
    public static boolean tryClaim() {
        String executionId = parseExecutionId(XxlJobHelper.getJobParam());
        // 没有携带执行 ID（如旧版引擎直连、或参数解析失败）：无法去重，
        // 采取"宁可多执行、不可漏执行"的降级策略，放行。
        if (executionId == null) {
            return true;
        }

        // 原子占位：putIfAbsent 仅在 key 不存在时写入并返回 null；
        // 返回非 null = key 已存在 = 重复派发。
        if (CLAIMED.putIfAbsent(executionId, Boolean.TRUE) != null) {
            XxlJobHelper.log("⚠️ 幂等拦截：执行ID={} 已处理过，本次重复派发直接跳过", executionId);
            System.out.println("⚠️ [幂等拦截] 执行ID=" + executionId + " 已处理过，跳过重复派发！");
            return false;
        }
        XxlJobHelper.log("✅ 原子占位成功，开始执行，执行ID={}", executionId);
        return true;
    }

    /**
     * 从 Go 引擎透传的 executorParams（JSON 字符串）里解析 executionId。
     * Go 端 fireOnce 拼的格式：{"executionId":"<jobID>:<slot>"}
     * Java 端通过 XxlJobHelper.getJobParam() 拿到这段字符串。
     */
    private static String parseExecutionId(String executorParams) {
        if (executorParams == null || executorParams.isBlank()) {
            return null;
        }
        try {
            // 避免引入 Jackson 依赖，用最朴素的字符串切分解析
            int keyIdx = executorParams.indexOf("\"executionId\"");
            if (keyIdx < 0) {
                return null;
            }
            int colonIdx = executorParams.indexOf(':', keyIdx);
            int beginQuote = executorParams.indexOf('"', colonIdx + 1);
            int endQuote = executorParams.indexOf('"', beginQuote + 1);
            if (beginQuote < 0 || endQuote < 0) {
                return null;
            }
            return executorParams.substring(beginQuote + 1, endQuote);
        } catch (Exception e) {
            // 解析异常 → 降级放行（见 tryClaim 注释）
            return null;
        }
    }
}
