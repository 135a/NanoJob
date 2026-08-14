package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLStore MySQL 实现的持久层, 同时承载任务配置与执行日志。
// 相比 etcd: 存储层单点, 但应用层多副本 HA, 无需 Raft 协调成本。
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore 连接 MySQL。dsn 形如:
//
//	"root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4&parseTime=true"
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("打开 MySQL 失败: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("连接 MySQL 失败: %v", err)
	}
	return &MySQLStore{db: db}, nil
}

// EnsureTables 幂等建表 (任务配置 + 执行日志)。
func (s *MySQLStore) EnsureTables(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS nanojob_job (
			id                BIGINT AUTO_INCREMENT PRIMARY KEY,
			cron              VARCHAR(64)  NOT NULL,
			executor_handler  VARCHAR(128) NOT NULL,
			app_name          VARCHAR(64)  NOT NULL,
			next_trigger_time BIGINT       NOT NULL DEFAULT 0,
			created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		return fmt.Errorf("建 nanojob_job 表失败: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
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
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		return fmt.Errorf("建 nanojob_log 表失败: %v", err)
	}

	return nil
}

// ---- 任务配置 ----

// CreateJob 新增任务, 返回 MySQL 自增 ID。
func (s *MySQLStore) CreateJob(ctx context.Context, job *JobInfo) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO nanojob_job (cron, executor_handler, app_name, next_trigger_time) VALUES (?, ?, ?, ?)`,
		job.Cron, job.ExecutorHandler, job.AppName, job.NextTriggerTime)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	job.ID = id
	return id, nil
}

// SaveJob 更新任务 (主要写回下一周期 NextTriggerTime)。
func (s *MySQLStore) SaveJob(ctx context.Context, job *JobInfo) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nanojob_job SET cron=?, executor_handler=?, app_name=?, next_trigger_time=? WHERE id=?`,
		job.Cron, job.ExecutorHandler, job.AppName, job.NextTriggerTime, job.ID)
	return err
}

func (s *MySQLStore) GetJob(ctx context.Context, id int64) (*JobInfo, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, cron, executor_handler, app_name, next_trigger_time FROM nanojob_job WHERE id=?`, id)
	var j JobInfo
	if err := row.Scan(&j.ID, &j.Cron, &j.ExecutorHandler, &j.AppName, &j.NextTriggerTime); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("未找到任务: %d", id)
		}
		return nil, err
	}
	return &j, nil
}

func (s *MySQLStore) ListJobs(ctx context.Context) ([]*JobInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, cron, executor_handler, app_name, next_trigger_time FROM nanojob_job`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*JobInfo
	for rows.Next() {
		var j JobInfo
		if err := rows.Scan(&j.ID, &j.Cron, &j.ExecutorHandler, &j.AppName, &j.NextTriggerTime); err != nil {
			return nil, err
		}
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

func (s *MySQLStore) DeleteJob(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM nanojob_job WHERE id=?`, id)
	return err
}

// ---- 执行日志 ----

// CreateLog 触发前插入一行 (handle_code=0 表示运行中), 返回 logId。
func (s *MySQLStore) CreateLog(ctx context.Context, log *JobLog) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO nanojob_log (job_id, app_name, executor_handler, exec_id, trigger_time, trigger_ip)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		log.JobID, log.AppName, log.ExecutorHandler, log.ExecID, log.TriggerTime, log.TriggerIP)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	log.ID = id
	return id, nil
}

// UpdateLog 回调回填结果。按 log_id 更新天然幂等, 重复回调覆盖即可。
func (s *MySQLStore) UpdateLog(ctx context.Context, logID int64, handleCode int, handleMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nanojob_log SET handle_code=?, handle_msg=?, callback_time=? WHERE id=?`,
		handleCode, handleMsg, time.Now().Unix(), logID)
	return err
}

func (s *MySQLStore) ListLogs(ctx context.Context, jobID int64) ([]*JobLog, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, app_name, executor_handler, exec_id, trigger_time, trigger_ip,
		        handle_code, IFNULL(handle_msg,''), callback_time
		   FROM nanojob_log WHERE job_id=? ORDER BY id DESC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*JobLog
	for rows.Next() {
		var l JobLog
		if err := rows.Scan(&l.ID, &l.JobID, &l.AppName, &l.ExecutorHandler, &l.ExecID,
			&l.TriggerTime, &l.TriggerIP, &l.HandleCode, &l.HandleMsg, &l.CallbackTime); err != nil {
			return nil, err
		}
		logs = append(logs, &l)
	}
	return logs, rows.Err()
}
