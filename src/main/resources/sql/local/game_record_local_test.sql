-- ==========================================
-- ODS 层：游戏记录数据接入（本地测试版）
-- 职责：从 MySQL CDC 读取原始数据，输出到控制台
-- 用途：本地开发调试，不依赖 Kafka 和 HDFS
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- 状态后端：使用内存（轻量级测试，不依赖 HDFS）
SET 'state.backend' = 'hashmap';

-- 并行度
SET 'table.exec.resource.default-parallelism' = '1';

-- 1. 定义源表 (MySQL CDC)
CREATE TABLE source_game_record_local (
  id BIGINT,
  user_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  bet_amount DECIMAL(19, 2),
  settle_amount DECIMAL(19, 2),
  transfer_amount DECIMAL(19, 2),
  bet_deposit_amount DECIMAL(19, 2),
  bet_withdrawal_amount DECIMAL(19, 2),
  bet_bonus_amount DECIMAL(19, 2),
  effective_bet_flag TINYINT,
  settle_deposit_amount DECIMAL(19, 2),
  settle_withdrawal_amount DECIMAL(19, 2),
  settle_bonus_amount DECIMAL(19, 2),
  transfer_deposit_amount DECIMAL(19, 2),
  transfer_withdrawal_amount DECIMAL(19, 2),
  transfer_bonus_amount DECIMAL(19, 2),
  third_platform_provider_type STRING,
  bet_time TIMESTAMP(3),
  settle_time TIMESTAMP(3),
  status STRING,
  bet_no STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  game_name_code STRING,
  order_no BIGINT,
  channel_id BIGINT,
  game_record_expand_list STRING,
  valid_bet_amount DECIMAL(19, 2),
  valid_bet_deposit_amount DECIMAL(19, 2),
  valid_bet_withdrawal_amount DECIMAL(19, 2),
  valid_bet_bonus_amount DECIMAL(19, 2),
  third_provider_game_code STRING,
  parent_game_category_id BIGINT,
  third_provider_game_type BIGINT,
  `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',  -- MySQL 主机地址
    'port' = '${MYSQL_PORT}',  -- MySQL 端口
    'username' = 'super',             -- MySQL 用户名
    'password' = '${MYSQL_PASSWORD}',  -- 从环境变量读取密码
    'database-name' = 'statics',      -- 数据库名
    'table-name' = 'game_record_.*',
    'server-id' = '6501-6502',
    'server-time-zone' = 'Asia/Bangkok',  -- MySQL 服务器实际时区是 UTC+7（曼谷时间）
    'scan.startup.mode' = 'latest-offset'
  );

-- 2. 定义 Print Sink (输出到控制台，不依赖 Kafka)
CREATE TABLE sink_print (
  id BIGINT,
  user_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  bet_amount DECIMAL(19, 2),
  settle_amount DECIMAL(19, 2),
  transfer_amount DECIMAL(19, 2),
  bet_deposit_amount DECIMAL(19, 2),
  bet_withdrawal_amount DECIMAL(19, 2),
  bet_bonus_amount DECIMAL(19, 2),
  effective_bet_flag TINYINT,
  settle_deposit_amount DECIMAL(19, 2),
  settle_withdrawal_amount DECIMAL(19, 2),
  settle_bonus_amount DECIMAL(19, 2),
  transfer_deposit_amount DECIMAL(19, 2),
  transfer_withdrawal_amount DECIMAL(19, 2),
  transfer_bonus_amount DECIMAL(19, 2),
  third_platform_provider_type STRING,
  bet_time TIMESTAMP(3),
  settle_time TIMESTAMP(3),
  status STRING,
  bet_no STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  game_name_code STRING,
  order_no BIGINT,
  channel_id BIGINT,
  game_record_expand_list STRING,
  valid_bet_amount DECIMAL(19, 2),
  valid_bet_deposit_amount DECIMAL(19, 2),
  valid_bet_withdrawal_amount DECIMAL(19, 2),
  valid_bet_bonus_amount DECIMAL(19, 2),
  third_provider_game_code STRING,
  parent_game_category_id BIGINT,
  third_provider_game_type BIGINT
)
WITH
  (
    'connector' = 'print',
    'standard-error' = 'false'
  );

-- 3. 提交任务（数据会打印到控制台）
INSERT INTO
  sink_print
SELECT
  id,
  user_id,
  game_id,
  game_category_id,
  bet_amount,
  settle_amount,
  transfer_amount,
  bet_deposit_amount,
  bet_withdrawal_amount,
  bet_bonus_amount,
  effective_bet_flag,
  settle_deposit_amount,
  settle_withdrawal_amount,
  settle_bonus_amount,
  transfer_deposit_amount,
  transfer_withdrawal_amount,
  transfer_bonus_amount,
  third_platform_provider_type,
  bet_time,
  settle_time,
  status,
  bet_no,
  create_time,
  update_time,
  game_name_code,
  order_no,
  channel_id,
  game_record_expand_list,
  valid_bet_amount,
  valid_bet_deposit_amount,
  valid_bet_withdrawal_amount,
  valid_bet_bonus_amount,
  third_provider_game_code,
  parent_game_category_id,
  third_provider_game_type
FROM
  source_game_record_local
WHERE `row_status` <> '-D';

