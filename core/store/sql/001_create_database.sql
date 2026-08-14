-- NanoJob 建库脚本 (引擎启动时自举执行)
-- 库名取自配置 DSN (conf.json mysql.dsn), 由代码替换 {database} 占位符, 保证与配置一致;
-- docker-compose 部署时改用 mysql 镜像的 MYSQL_DATABASE=nanojob 环境变量建库, 两者等效。
CREATE DATABASE IF NOT EXISTS `{database}` DEFAULT CHARACTER SET utf8mb4;
