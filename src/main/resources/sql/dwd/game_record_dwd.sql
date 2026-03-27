-- ==========================================
-- DWD层：游戏记录数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   MySQL.game_record (业务库)
--       ↓ CDC (ods/game_record_all_to_kafka.sql)
--   ODS: flink_game_order_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_game_record (Kafka)
--       ↓ Flink SQL 聚合 (dws/kafka_to_game.sql)
--   DWS: game_record, today_user_game_statistics_dws, today_user_hour_game_ug_statistics_data,
--        today_channel_game_category_statistic, today_channel_game_provider_statistic,
--        today_channel_game_record_statistic (PolarDB)
--
-- 上游表：flink_game_order_data_sync (Kafka)
-- 上游文件：ods/game_record_all_to_kafka.sql
-- 下游表：dwd_game_record (Kafka)
-- 下游文件：dws/kafka_to_game.sql
-- ==========================================

-- 1. 核心配置
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.exec.state.ttl' = '36h';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'execution.checkpointing.interval' = '3min';
SET 'table.local-time-zone' = 'Asia/Singapore';

-- 2. 定义ODS层源表 (从Kafka读取原始数据)
CREATE TABLE ods_game_record (
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
  PRIMARY KEY (order_no, create_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'flink_game_order_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_game_record_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_game_record (
  -- 维度字段
  statistic_time DATE,
  statistic_hour BIGINT,
  user_id BIGINT,
  channel_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  third_platform_provider_type STRING,
  
  -- 事实字段
  id BIGINT,
  order_no BIGINT,
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
  valid_bet_amount DECIMAL(19, 2),
  valid_bet_deposit_amount DECIMAL(19, 2),
  valid_bet_withdrawal_amount DECIMAL(19, 2),
  valid_bet_bonus_amount DECIMAL(19, 2),
  
  -- 状态字段
  status STRING,
  
  -- 时间字段
  update_time TIMESTAMP(3),
  
  PRIMARY KEY (order_no, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_game_record',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_game_record_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_game_record
SELECT
  -- 计算统计日期和小时
  CAST(update_time AS DATE) AS statistic_time,
  HOUR(update_time) AS statistic_hour,
  
  -- 维度字段
  user_id,
  channel_id,
  game_id,
  game_category_id,
  third_platform_provider_type,
  
  -- 事实字段
  id,
  order_no,
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
  valid_bet_amount,
  valid_bet_deposit_amount,
  valid_bet_withdrawal_amount,
  valid_bet_bonus_amount,
  
  -- 状态字段
  status,
  
  -- 时间字段
  update_time
FROM ods_game_record
WHERE status = 'settle'
  AND order_no IS NOT NULL
  AND update_time IS NOT NULL;
