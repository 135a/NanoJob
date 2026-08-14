-- NanoJob 建表脚本 (引擎启动时自动执行, IF NOT EXISTS 幂等)
-- 任务表: 任务配置 + 下次触发时间 (调度引擎的"记忆", 故障转移后新 Leader 靠它恢复挂轮子)
CREATE TABLE IF NOT EXISTS nanojob_job (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    cron              VARCHAR(64)  NOT NULL,
    executor_handler  VARCHAR(128) NOT NULL,
    app_name          VARCHAR(64)  NOT NULL,
    next_trigger_time BIGINT       NOT NULL DEFAULT 0,
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 执行日志表: 每次触发的派发信息 + Java 回调回填的执行结果
CREATE TABLE IF NOT EXISTS nanojob_log (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id           BIGINT       NOT NULL,
    app_name         VARCHAR(64)  NOT NULL DEFAULT '',
    executor_handler VARCHAR(128) NOT NULL DEFAULT '',
    exec_id          VARCHAR(128) NOT NULL DEFAULT '',
    trigger_time     BIGINT       NOT NULL DEFAULT 0,
    trigger_ip       VARCHAR(128) NOT NULL DEFAULT '',
    handle_code      INT          NOT NULL DEFAULT 0,
    handle_msg       TEXT,
    callback_time    BIGINT       NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
