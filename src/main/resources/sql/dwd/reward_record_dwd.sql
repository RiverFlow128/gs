-- ==========================================
-- DWD层：奖励记录数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   业务系统直写Kafka (非MySQL CDC接入，无源数据库)
--   原因：奖励记录由业务系统实时推送，不经过MySQL落库
--       ↓
--   ODS: reward_record_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_reward_record (Kafka)
--       ↓ Flink SQL 聚合
--   DWS: dws/reward_record_kafka_to_polardb.sql
--   ADS: ads/today_user_reward_statistic.sql
--
-- 上游表：reward_record_data_sync (Kafka) - 业务系统直写
-- 上游文件：无（业务系统直接写入Kafka）
-- 下游表：dwd_reward_record (Kafka)
-- 下游文件：dws/reward_record_kafka_to_polardb.sql, ads/today_user_reward_statistic.sql
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
CREATE TABLE ods_reward_record (
  `key` STRING,
  `dataSyncType` STRING,
  `jsonData` ROW<
    id BIGINT,
    user_id BIGINT,
    channel_id BIGINT,
    unique_id BIGINT,
    reward_scope STRING,
    reward_type STRING,
    activity_type STRING,
    bonus_amount DECIMAL(20, 4),
    draw_flag BOOLEAN,
    not_receive_time TIMESTAMP(3),
    receive_time TIMESTAMP(3),
    time_out_time TIMESTAMP(3),
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3)
  >,
  PRIMARY KEY (`key`) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'reward_record_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_reward_record_group',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_reward_record (
  -- 维度字段
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  unique_id BIGINT,
  reward_scope STRING,
  reward_type STRING,
  activity_type STRING,
  
  -- 事实字段
  id BIGINT,
  bonus_amount DECIMAL(20, 4),
  draw_flag INT,
  
  -- 时间字段
  not_receive_time TIMESTAMP(3),
  receive_time TIMESTAMP(3),
  time_out_time TIMESTAMP(3),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  
  PRIMARY KEY (unique_id, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_reward_record',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_reward_record_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_reward_record
SELECT
  -- 计算统计日期
  CAST(jsonData.update_time AS DATE) AS statistic_time,
  
  -- 维度字段
  jsonData.user_id,
  jsonData.channel_id,
  jsonData.unique_id,
  jsonData.reward_scope,
  jsonData.reward_type,
  jsonData.activity_type,
  
  -- 事实字段
  jsonData.id,
  jsonData.bonus_amount,
  IF(jsonData.draw_flag, 1, 0) AS draw_flag,
  
  -- 时间字段
  jsonData.not_receive_time,
  jsonData.receive_time,
  jsonData.time_out_time,
  jsonData.create_time,
  jsonData.update_time
FROM ods_reward_record
WHERE jsonData.update_time IS NOT NULL
  AND jsonData.reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS');
