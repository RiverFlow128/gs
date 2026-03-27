-- ==========================================
-- DWD层：人工增减记录数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   MySQL.amount_operation_record (业务库)
--       ↓ CDC (ods/backend_amount_operation_record_to_kafka.sql)
--   ODS: flink_amount_operation_record_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_amount_operation_record (Kafka)
--       ↓ Flink SQL 聚合 (dws/kafka_to_backend_amount_operation_record.sql)
--   DWS: today_user_custom_amount_statistics_data, today_channel_custom_amount_statistic (PolarDB)
--
-- 上游表：flink_amount_operation_record_data_sync (Kafka)
-- 上游文件：ods/backend_amount_operation_record_to_kafka.sql
-- 下游表：dwd_amount_operation_record (Kafka)
-- 下游文件：dws/kafka_to_backend_amount_operation_record.sql
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
CREATE TABLE ods_amount_operation_record (
  id BIGINT,
  user_id BIGINT,
  `type` STRING,
  amount_type STRING,
  amount DECIMAL(19, 4),
  balance DECIMAL(19, 4),
  status STRING,
  create_by STRING,
  update_by STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  channel_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'flink_amount_operation_record_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_amount_operation_record_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_amount_operation_record (
  -- 维度字段
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  
  -- 事实字段
  id BIGINT,
  `type` STRING,
  amount_type STRING,
  amount DECIMAL(19, 4),
  balance DECIMAL(19, 4),
  
  -- 状态字段
  status STRING,
  
  -- 时间字段
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  
  PRIMARY KEY (id, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_amount_operation_record',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_amount_operation_record_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_amount_operation_record
SELECT
  -- 计算统计日期
  CAST(update_time AS DATE) AS statistic_time,
  
  -- 维度字段
  user_id,
  channel_id,
  
  -- 事实字段
  id,
  `type`,
  amount_type,
  amount,
  balance,
  
  -- 状态字段
  status,
  
  -- 时间字段
  create_time,
  update_time
FROM ods_amount_operation_record
WHERE status = 'COMPLETE'
  AND update_time IS NOT NULL;
