-- ==========================================
-- DIM层：用户维度表同步
-- 职责：从MySQLCDC读取用户数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user (业务库)
--       ↓ CDC [本文件]
--   DIM: user (PolarDB statics库)
--
-- 上游表：MySQL.user - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 原配置（仅用于开发）
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 修改为：使用 OSS 分布式存储

-- 进阶优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

-- 1.数据库表信息
CREATE TABLE user_source (
  id BIGINT,
  user_id BIGINT,
  user_phone STRING,
  password STRING,
  register_ip STRING,
  register_device_id STRING,
  user_country STRING,
  user_status INT,
  register_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  user_email STRING,
  phone_area_code STRING,
  freeze_time TIMESTAMP(3),
  test_flag INT,
  app_channel STRING,
  register_source_value STRING,
  register_source_type STRING,
  abs_id BIGINT,
  channel_id BIGINT,
  register_method STRING,
  last_login_ip STRING,
  last_login_time TIMESTAMP(3),
  last_login_device_id STRING,
  user_type INT,
  tourist_name STRING,
  tourist_must_bind_user INT,
  PRIMARY KEY (user_id, register_time) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'user_basic-test',
    'table-name' = 'user',
    'server-id' = '16581-16600', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'initial'
  );

-- 3. 定义 PolarDB 目标表
CREATE TABLE polardb_user (
  id BIGINT,
  user_id BIGINT,
  user_phone STRING,
  password STRING,
  register_ip STRING,
  register_device_id STRING,
  user_country STRING,
  user_status INT,
  register_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  user_email STRING,
  phone_area_code STRING,
  freeze_time TIMESTAMP(3),
  test_flag INT,
  app_channel STRING,
  register_source_value STRING,
  register_source_type STRING,
  abs_id BIGINT,
  channel_id BIGINT,
  register_method STRING,
  last_login_ip STRING,
  last_login_time TIMESTAMP(3),
  last_login_device_id STRING,
  user_type INT,
  tourist_name STRING,
  tourist_must_bind_user INT,
  PRIMARY KEY (user_id, register_time) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'user',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    -- 1. 启用批量写入（注意移除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    -- 3. 重试策略（去掉 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
        
    -- JDBC Sink 默认 At-Least-Once，无法保证 Exactly-Once 语义
    -- 解决方案：在INSERT语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

  );

-- 3. 提交任务
INSERT INTO
  polardb_user
SELECT
  *
FROM
  user_source;
