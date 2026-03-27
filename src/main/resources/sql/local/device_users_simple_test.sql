-- ==========================================
-- ODS 层：设备用户关系表（极简测试版）
-- 用途：验证 MySQL CDC 是否能正常工作
-- ==========================================

-- 不配置 Checkpoint，避免重启问题
-- SET 'execution.checkpointing.interval' = '10s';

-- 简单设置
SET 'table.exec.resource.default-parallelism' = '1';
SET 'state.backend' = 'hashmap';

-- 1. 定义源表
CREATE TABLE source_device_users (
  id BIGINT,
  device_id STRING,
  user_id BIGINT,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',  -- 从环境变量读取密码
    'database-name' = 'statics',
    'table-name' = 'device_users_relations_flat',
    'server-id' = '6501-6502',
    'server-time-zone' = 'Asia/Bangkok',  -- MySQL 服务器实际时区是 UTC+7
    'scan.startup.mode' = 'latest-offset'
  );

-- 2. 定义 Print Sink
CREATE TABLE sink_print_simple (
  id BIGINT,
  device_id STRING,
  user_id BIGINT,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3)
)
WITH
  (
    'connector' = 'print',
    'standard-error' = 'false'
  );

-- 3. 提交任务
INSERT INTO sink_print_simple
SELECT
  id,
  device_id,
  user_id,
  create_time,
  update_time
FROM source_device_users
WHERE `row_status` <> '-D';

