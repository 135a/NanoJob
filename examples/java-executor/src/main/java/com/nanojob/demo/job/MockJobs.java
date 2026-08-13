package com.nanojob.demo.job;

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Component
public class MockJobs {

    /**
     * 示例 1：模拟海量数据分片广播 (展示 NanoJob 核心 Sharding 能力)
     */
    @XxlJob("loanInterestJobHandler")
    public void loanInterestJobHandler() throws Exception {
        // ⭐️ [架构修复 3] 执行器幂等去重：先按执行ID原子占位，重复派发直接跳过。
        //    为什么需要：本系统是 at-least-once 投递。旧主合法派发 slot N 后失联，
        //    新主接管时无法确认"旧主派没派"，会把它当成漏发再补一次 → 同一触发到两次。
        //    Go 引擎每次触发都带"确定性执行 ID"（任务ID:触发时间戳），
        //    交接期"旧主派发"和"新主补偿"拿到的是同一个 ID，这里就把第二次拦下来。
        //    详细机制 & 生产升级（DB唯一索引 / Redis SETNX）见 ExecutionDedup.java。
        if (!ExecutionDedup.tryClaim()) {
            return; // 重复派发，不执行业务
        }

        // 获取分片参数 (由 Go 引擎 Router 动态计算并下发)
        int shardIndex = XxlJobHelper.getShardIndex();
        int shardTotal = XxlJobHelper.getShardTotal();

        XxlJobHelper.log("收到 NanoJob 分片指令：Index={}, Total={}", shardIndex, shardTotal);
        
        System.out.printf("\n🚀 [信贷计息任务] 收到指令！我被分配处理第 %d/%d 份数据！\n", shardIndex, shardTotal);

        // 模拟执行耗时业务
        for (int i = 0; i < 5; i++) {
            System.out.println("   -> ⚙️ 正在猛烈计算: 用户 ID % " + shardTotal + " == " + shardIndex + " 的利息...");
            TimeUnit.MILLISECONDS.sleep(600);
        }
        
        System.out.println("✅ [信贷计息任务] 当前分片数据处理完毕！等待 Go 引擎下一次召唤。");
        XxlJobHelper.handleSuccess("计算利息完成！");
    }

    /**
     * 示例 2：模拟普通单机任务 (如定时清理日志)
     */
    @XxlJob("cleanupLogHandler")
    public void cleanupLogHandler() throws Exception {
        // ⭐️ [架构修复 3] 幂等去重，机制同 loanInterestJobHandler（见 ExecutionDedup.java）：
        //    先原子占位，同一执行 ID 重复到达直接跳过，保证 at-least-once 投递下只执行一次。
        if (!ExecutionDedup.tryClaim()) {
            return; // 重复派发，不执行业务
        }

        System.out.println("\n🧹 [日志清理任务] 收到指令！正在清理过期日志...");
        TimeUnit.SECONDS.sleep(2);
        System.out.println("✅ [日志清理任务] 清理完成！");
        XxlJobHelper.handleSuccess("清理成功");
    }
}
