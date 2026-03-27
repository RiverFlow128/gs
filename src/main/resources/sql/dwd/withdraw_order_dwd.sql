-- ==========================================
-- DWD层：提现订单数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   MySQL.withdraw_order (业务库)
--       ↓ CDC (ods/withdraw_order_to_kafka.sql)
--   ODS: flink_withdraw_order_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_withdraw_order (Kafka)
--       ↓ Flink SQL 聚合 (dws/withdraw_order_kafka_to_polardb.sql)
--   DWS: today_user_withdraw_statistic_dws, today_channel_withdraw_statistic,
--        today_user_withdraw_statistic_finance_increment (PolarDB)
--
-- 上游表：flink_withdraw_order_data_sync (Kafka)
-- 上游文件：ods/withdraw_order_to_kafka.sql
-- 下游表：dwd_withdraw_order (Kafka)
-- 下游文件：dws/withdraw_order_kafka_to_polardb.sql
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
CREATE TABLE ods_withdraw_order (
  id BIGINT,
  order_no BIGINT,
  user_id BIGINT,
  channel_id BIGINT,
  channel STRING,
  withdraw_funds_amount DECIMAL(20, 4),
  balance DECIMAL(20, 4),
  withdrawal_amount DECIMAL(20, 4),
  projected_amount DECIMAL(20, 4),
  platform_handling_fee DECIMAL(20, 4),
  final_handling_fee DECIMAL(20, 4),
  final_withdraw_funds_amount DECIMAL(20, 4),
  actual_amount DECIMAL(20, 4),
  handling_type STRING,
  handling_mode STRING,
  handling_detail STRING,
  gift_mode STRING,
  gift_value DECIMAL(20, 4),
  gift_amount DECIMAL(20, 4),
  decrease_handling_mode STRING,
  decrease_handling_value DECIMAL(20, 4),
  decrease_handling_amount DECIMAL(20, 4),
  apply_ip STRING,
  user_bank_id BIGINT,
  apply_name STRING,
  apply_bank_name STRING,
  apply_bank_id BIGINT,
  apply_bank_card STRING,
  apply_email STRING,
  apply_phone STRING,
  order_status STRING,
  order_status_event STRING,
  remark STRING,
  reasons STRING,
  front_remarks STRING,
  back_remarks STRING,
  pay_provider_id BIGINT,
  withdrawal_mode_id BIGINT,
  withdrawal_mode_name STRING,
  withdrawal_channel_id BIGINT,
  withdrawal_channel_name STRING,
  withdrawal_channel_bank_code STRING,
  pay_provider_type STRING,
  payment_order_no STRING,
  third_party_order_no STRING,
  third_party_handling_fee DECIMAL(20, 4),
  payments_number BIGINT,
  first_withdrawal_flag TINYINT,
  create_by BIGINT,
  create_time TIMESTAMP(3),
  update_by BIGINT,
  update_time TIMESTAMP(3),
  request_time TIMESTAMP(3),
  fallback_time TIMESTAMP(3),
  payout_successful_time TIMESTAMP(3),
  locked_user_id BIGINT,
  last_audit_user BIGINT,
  last_audit_time TIMESTAMP(3),
  PRIMARY KEY (order_no, create_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'flink_withdraw_order_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_withdraw_order_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_withdraw_order (
  -- 维度字段
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  apply_bank_id BIGINT,
  withdrawal_mode_id BIGINT,
  withdrawal_channel_id BIGINT,
  pay_provider_id BIGINT,
  
  -- 事实字段
  id BIGINT,
  order_no BIGINT,
  withdraw_funds_amount DECIMAL(20, 4),
  final_handling_fee DECIMAL(20, 4),
  gift_amount DECIMAL(20, 4),
  decrease_handling_amount DECIMAL(20, 4),
  first_withdrawal_flag TINYINT,
  
  -- 状态字段
  order_status STRING,
  
  -- 时间字段
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  
  PRIMARY KEY (order_no, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_withdraw_order',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_withdraw_order_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_withdraw_order
SELECT
  -- 计算统计日期
  CAST(update_time AS DATE) AS statistic_time,
  
  -- 维度字段
  user_id,
  channel_id,
  apply_bank_id,
  withdrawal_mode_id,
  withdrawal_channel_id,
  pay_provider_id,
  
  -- 事实字段
  id,
  order_no,
  withdraw_funds_amount,
  final_handling_fee,
  gift_amount,
  decrease_handling_amount,
  first_withdrawal_flag,
  
  -- 状态字段
  order_status,
  
  -- 时间字段
  create_time,
  update_time
FROM ods_withdraw_order
WHERE order_no IS NOT NULL
  AND update_time IS NOT NULL;
