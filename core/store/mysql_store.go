package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// go:embed sql/*.sql 把建库建表 DDL 打进二进制, 引擎启动时自动执行。
// SQL 以独立 .sql 文件管理 (可读可改可 diff), 不再散落在 Go 字符串里 (解耦)。
//
//go:embed sql/*.sql
var sqlFS embed.FS

// MySQLStore MySQL 实现的持久层, 同时承载任务配置与执行日志。
// 相比 etcd: 存储层单点, 但应用层多副本 HA, 无需 Raft 协调成本。
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore 连接 MySQL。dsn 形如:
//
//	"root:123456@tcp(127.0.0.1:3306)/nanojob?charset=utf8mb4&parseTime=true"
//
// 先自举建库: 库可能还不存在, 先用去掉库名的 DSN 连一次服务器执行建库 SQL,
// 再以完整 DSN 连接。这让源码启动也"开箱即用", 不必手动执行建库命令。
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	if err := ensureDatabase(dsn); err != nil {
		return nil, err
	}

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

// ensureDatabase 自举建库: 库不存在时, 用去掉库名的 DSN 连一次服务器, 执行 sql/001 的建库 SQL。
// 库名从 DSN 解析并替换 SQL 里的 {database} 占位符, 保证与 conf.json 配置一致。
func ensureDatabase(dsn string) error {
	ddl, err := sqlFS.ReadFile("sql/001_create_database.sql")
	if err != nil {
		return fmt.Errorf("读取建库 SQL 失败: %v", err)
	}

	dbName := dsnDatabase(dsn)
	if dbName == "" {
		return fmt.Errorf("DSN 中缺少库名, 无法自举建库: %s", dsn)
	}
	stmt := strings.ReplaceAll(string(ddl), "{database}", dbName)

	// 不带库名连服务器 (库还不存在, 带库名会连不上)
	boot, err := sql.Open("mysql", dsnWithoutDatabase(dsn))
	if err != nil {
		return fmt.Errorf("打开 MySQL 自举连接失败: %v", err)
	}
	defer boot.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := boot.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("自举建库失败 (库 %s): %v", dbName, err)
	}
	return nil
}

// dsnDatabase 从 DSN 解析库名: "user:pass@tcp(host:port)/dbname?params" → "dbname"
func dsnDatabase(dsn string) string {
	i := strings.LastIndex(dsn, "/")
	if i < 0 {
		return ""
	}
	rest := dsn[i+1:]
	if j := strings.Index(rest, "?"); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

// dsnWithoutDatabase 去掉 DSN 里的库名段 (连服务器但不选库):
// "user:pass@tcp(host:port)/dbname?params" → "user:pass@tcp(host:port)/?params"
func dsnWithoutDatabase(dsn string) string {
	i := strings.LastIndex(dsn, "/")
	if i < 0 {
		return dsn
	}
	if j := strings.Index(dsn[i:], "?"); j >= 0 {
		return dsn[:i+1] + dsn[i+j:]
	}
	return dsn[:i+1]
}

// splitStatements 按分号切分 SQL 文件为单条语句, 忽略空串。
// 本项目 DDL 无内嵌分号, 简单切分足够; 含 -- 注释行也能整体执行 (MySQL 忽略注释)。
func splitStatements(ddl string) []string {
	var stmts []string
	for _, s := range strings.Split(ddl, ";") {
		if s = strings.TrimSpace(s); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

// EnsureTables 幂等建表 (任务配置 + 执行日志): 读取嵌入的 sql/002_create_tables.sql 逐条执行。
// IF NOT EXISTS 保证重复启动无副作用。只建表不迁移, 加字段需另行编写 ALTER (README 已知限制)。
func (s *MySQLStore) EnsureTables(ctx context.Context) error {
	ddl, err := sqlFS.ReadFile("sql/002_create_tables.sql")
	if err != nil {
		return fmt.Errorf("读取建表 SQL 失败: %v", err)
	}
	for _, stmt := range splitStatements(string(ddl)) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("执行建表 SQL 失败: %v", err)
		}
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
