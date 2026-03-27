-- ==========================================
-- DWD层：代理统计数据清洗
-- 职责：从ODS层读取原始数据，进行数据清洗后写入DWD层Kafka
--
-- 数据血缘：
--   MySQL.user_proxy_statistic (业务库)
--       ↓ CDC (ods/user_proxy_statistic_to_kafka.sql)
--   ODS: flink_user_proxy_statistic_data_sync (Kafka)
--       ↓ Flink SQL 清洗 [本文件]
--   DWD: dwd_user_proxy_statistic (Kafka)
--       ↓ Flink SQL 聚合 (dws/kafka_to_user_proxy_statistic.sql)
--   DWS: today_channel_proxy_statistic (PolarDB)
--
-- 上游表：flink_user_proxy_statistic_data_sync (Kafka)
-- 上游文件：ods/user_proxy_statistic_to_kafka.sql
-- 下游表：dwd_user_proxy_statistic (Kafka)
-- 下游文件：dws/kafka_to_user_proxy_statistic.sql
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
CREATE TABLE ods_user_proxy_statistic (
  `id` BIGINT,
  `user_id` BIGINT,
  `channel_id` BIGINT,
  `statistic_time` DATE,
  `direct_bet_rebate_amount` DECIMAL(20, 4),
  `direct_recharge_rebate_amount` DECIMAL(20, 4),
  `teams_bet_rebate_amount` DECIMAL(20, 4),
  `teams_recharge_rebate_amount` DECIMAL(20, 4),
  `team_size` INT,
  `direct_member` INT,
  `valid_direct_member` INT,
  `transfer_amount` DECIMAL(20, 4),
  `transfer_count` INT,
  `invitation_amount` DECIMAL(20, 4),
  `achievement_amount` DECIMAL(20, 4),
  `rebate_settlement` DECIMAL(20, 4),
  `betting_rebate_count` INT,
  `recharge_rebate_count` INT,
  `effective_bet_amount` DECIMAL(20, 4),
  `team_effective_bet_amount` DECIMAL(20, 4),
  `direct_effective_bet_amount` DECIMAL(20, 4),
  `teams_rebate_amount` DECIMAL(20, 4),
  `obtain_betting_rebate_count` INT,
  `obtain_recharge_rebate_count` INT,
  `create_time` TIMESTAMP(3),
  `update_time` TIMESTAMP(6)
)
WITH (
  'connector' = 'kafka',
  'topic' = 'flink_user_proxy_statistic_data_sync',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_user_proxy_statistic_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义DWD层目标表 (清洗后写入Kafka)
CREATE TABLE dwd_user_proxy_statistic (
  -- 维度字段
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  
  -- 事实字段
  id BIGINT,
  direct_bet_rebate_amount DECIMAL(20, 4),
  direct_recharge_rebate_amount DECIMAL(20, 4),
  teams_bet_rebate_amount DECIMAL(20, 4),
  teams_recharge_rebate_amount DECIMAL(20, 4),
  team_size INT,
  direct_member INT,
  valid_direct_member INT,
  transfer_amount DECIMAL(20, 4),
  transfer_count INT,
  invitation_amount DECIMAL(20, 4),
  achievement_amount DECIMAL(20, 4),
  rebate_settlement DECIMAL(20, 4),
  betting_rebate_count INT,
  recharge_rebate_count INT,
  effective_bet_amount DECIMAL(20, 4),
  team_effective_bet_amount DECIMAL(20, 4),
  direct_effective_bet_amount DECIMAL(20, 4),
  teams_rebate_amount DECIMAL(20, 4),
  obtain_betting_rebate_count INT,
  obtain_recharge_rebate_count INT,
  
  -- 时间字段
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(6),
  
  PRIMARY KEY (id, update_time) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_proxy_statistic',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dwd_user_proxy_statistic_producer_group',
  'format' = 'debezium-json',
  'sink.parallelism' = '3'
);

-- 4. 数据清洗逻辑
INSERT INTO dwd_user_proxy_statistic
SELECT
  -- 维度字段
  statistic_time,
  user_id,
  channel_id,
  
  -- 事实字段
  id,
  direct_bet_rebate_amount,
  direct_recharge_rebate_amount,
  teams_bet_rebate_amount,
  teams_recharge_rebate_amount,
  team_size,
  direct_member,
  valid_direct_member,
  transfer_amount,
  transfer_count,
  invitation_amount,
  achievement_amount,
  rebate_settlement,
  betting_rebate_count,
  recharge_rebate_count,
  effective_bet_amount,
  team_effective_bet_amount,
  direct_effective_bet_amount,
  teams_rebate_amount,
  obtain_betting_rebate_count,
  obtain_recharge_rebate_count,
  
  -- 时间字段
  create_time,
  update_time
FROM ods_user_proxy_statistic
WHERE update_time IS NOT NULL;
