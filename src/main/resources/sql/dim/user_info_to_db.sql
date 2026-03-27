-- ==========================================
-- DIM层：用户信息维度表同步
-- 职责：从MySQLCDC读取用户信息数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_info (业务库)
--       ↓ CDC [本文件]
--   DIM: user_info (PolarDB statics库)
--
-- 上游表：MySQL.user_info - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_info (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 基础运行配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 进阶优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU 核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

CREATE TABLE source_user_info (
  id BIGINT,
  user_id BIGINT,
  nick_name STRING,
  user_avatar STRING,
  user_gender INT,
  user_age INT,
  vip_level INT,
  user_education STRING,
  user_brief STRING,
  user_date_birth STRING,
  user_location STRING,
  user_current_ip STRING,
  remark STRING,
  create_time TIMESTAMP,
  update_time TIMESTAMP(6),
  invite_code STRING,
  email_checked INT,
  update_nick_name_time DATE,
  update_nick_name_num INT,
  update_sign_num INT,
  update_sign_time DATE,
  current_level_recharge_amount DECIMAL(20, 4),
  current_level_effective_bet DECIMAL(20, 4),
  hidden_score INT,
  second_return INT,
  signature STRING,
  channel_id BIGINT,
  real_name STRING,
  bounty_arena_disabled_flag INT,
  game_valid_bet_calculate_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'user_basic-test',
    'table-name' = 'user_info',
    'server-id' = '2100-2300', -- 确保范围内有足够的 server-id 分配给并行任务
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'initial'
  );

CREATE TABLE target_user_info (
  id BIGINT,
  user_id BIGINT,
  nick_name STRING,
  user_avatar STRING,
  user_gender INT,
  user_age INT,
  vip_level INT,
  user_education STRING,
  user_brief STRING,
  user_date_birth STRING,
  user_location STRING,
  user_current_ip STRING,
  remark STRING,
  create_time TIMESTAMP,
  update_time TIMESTAMP(6),
  invite_code STRING,
  email_checked INT,
  update_nick_name_time DATE,
  update_nick_name_num INT,
  update_sign_num INT,
  update_sign_time DATE,
  current_level_recharge_amount DECIMAL(20, 4),
  current_level_effective_bet DECIMAL(20, 4),
  hidden_score INT,
  second_return INT,
  signature STRING,
  channel_id BIGINT,
  real_name STRING,
  bounty_arena_disabled_flag INT,
  game_valid_bet_calculate_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'user_info',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    -- 1. 开启缓冲写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
  );

INSERT INTO
  target_user_info
SELECT
  *
FROM
  source_user_info;