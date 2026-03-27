-- ==========================================
-- DWD层：充值订单数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   MySQL.recharge_order (业务库)
--       ↓ CDC (ods/recharge_order_to_kafka.sql)
--   ODS: flink_recharge_order_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_recharge_order (Kafka)
--       ↓ Flink SQL 聚合 (dws/recharge_order_kafka_to_polardb.sql)
--   DWS: today_user_recharge_statistic_dws, today_channel_recharge_statistic (PolarDB)
--
-- 上游表：flink_recharge_order_data_sync (Kafka)
-- 上游文件：ods/recharge_order_to_kafka.sql
-- 下游表：dwd_recharge_order (Kafka)
-- 下游文件：dws/recharge_order_kafka_to_polardb.sql
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
CREATE TABLE ods_recharge_order (
  id BIGINT,
  order_no BIGINT,
  user_id BIGINT,
  currency_code STRING,
  rate_scale DECIMAL(20, 4),
  order_amount DECIMAL(20, 4),
  pay_amount DECIMAL(20, 4),
  decrease_handling_fee DECIMAL(20, 4),
  handling_fee DECIMAL(20, 4),
  gift_amount DECIMAL(20, 4),
  recharge_amount DECIMAL(20, 4),
  actual_handling_fee DECIMAL(20, 4),
  actual_decrease_handling_fee DECIMAL(20, 4),
  actual_gift_amount DECIMAL(20, 4),
  actual_recharge_amount DECIMAL(20, 4),
  pay_status STRING,
  order_status STRING,
  first_recharge TINYINT,
  pay_method_id BIGINT,
  pay_category_id BIGINT,
  pay_channel_id BIGINT,
  pay_provider_id BIGINT,
  recharge_success_time TIMESTAMP(3),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  update_by STRING,
  channel_id BIGINT,
  PRIMARY KEY (order_no, create_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'flink_recharge_order_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_recharge_order_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_recharge_order (
  -- 维度字段
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  pay_category_id BIGINT,
  pay_method_id BIGINT,
  pay_channel_id BIGINT,
  pay_provider_id BIGINT,
  
  -- 事实字段
  id BIGINT,
  order_no BIGINT,
  currency_code STRING,
  rate_scale DECIMAL(20, 4),
  order_amount DECIMAL(20, 4),
  pay_amount DECIMAL(20, 4),
  decrease_handling_fee DECIMAL(20, 4),
  handling_fee DECIMAL(20, 4),
  gift_amount DECIMAL(20, 4),
  recharge_amount DECIMAL(20, 4),
  actual_handling_fee DECIMAL(20, 4),
  actual_decrease_handling_fee DECIMAL(20, 4),
  actual_gift_amount DECIMAL(20, 4),
  actual_recharge_amount DECIMAL(20, 4),
  first_recharge TINYINT,
  
  -- 状态字段
  pay_status STRING,
  order_status STRING,
  
  -- 时间字段
  recharge_success_time TIMESTAMP(3),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  
  PRIMARY KEY (order_no, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_recharge_order',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_recharge_order_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_recharge_order
SELECT
  -- 计算统计日期
  CAST(update_time AS DATE) AS statistic_time,
  
  -- 维度字段
  user_id,
  channel_id,
  pay_category_id,
  pay_method_id,
  pay_channel_id,
  pay_provider_id,
  
  -- 事实字段
  id,
  order_no,
  currency_code,
  rate_scale,
  order_amount,
  pay_amount,
  decrease_handling_fee,
  handling_fee,
  gift_amount,
  recharge_amount,
  actual_handling_fee,
  actual_decrease_handling_fee,
  actual_gift_amount,
  actual_recharge_amount,
  first_recharge,
  
  -- 状态字段
  pay_status,
  order_status,
  
  -- 时间字段
  recharge_success_time,
  create_time,
  update_time
FROM ods_recharge_order
WHERE order_no IS NOT NULL
  AND update_time IS NOT NULL;
