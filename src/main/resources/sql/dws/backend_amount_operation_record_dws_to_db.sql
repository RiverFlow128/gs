-- ==========================================
-- DWS层：金额操作记录聚合统计
-- 职责：从DWD层读取清洗后的数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_amount_operation_record (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: today_user_custom_amount_statistics_data, today_channel_custom_amount_statistic (PolarDB)
--
-- 上游表：dwd_amount_operation_record (Kafka)
-- 上游文件：dwd/amount_operation_record_dwd.sql
-- 下游表：today_user_custom_amount_statistics_data, today_channel_custom_amount_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- 1. 核心微批优化配置
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.exec.state.ttl' = '36h';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'execution.checkpointing.interval' = '3min';
SET 'table.local-time-zone' = 'Asia/Singapore';

-- 2. 定义DWD层源表 (从DWD层Kafka读取清洗后的数据)
CREATE TABLE dwd_amount_operation_record (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  id BIGINT,
  `type` STRING,
  amount_type STRING,
  amount DECIMAL(19, 4),
  balance DECIMAL(19, 4),
  status STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_amount_operation_record',
  'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
  'properties.group.id' = 'dws_amount_operation_record_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义用户维度统计 Sink 表 (PolarDB/MySQL)
CREATE TABLE sink_user_custom_amount_statistics (
  user_id BIGINT,
  channel_id BIGINT,
  statistic_time DATE,
  custom_increase_count BIGINT,
  custom_reduction_count BIGINT,
  custom_increase_amount DECIMAL(20, 4),
  custom_increase_withdraw_amount DECIMAL(20, 4),
  custom_increase_deposit_amount DECIMAL(20, 4),
  custom_increase_bonus_amount DECIMAL(20, 4),
  custom_reduction_amount DECIMAL(20, 4),
  custom_reduction_withdraw_amount DECIMAL(20, 4),
  custom_reduction_deposit_amount DECIMAL(20, 4),
  custom_reduction_bonus_amount DECIMAL(20, 4),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  PRIMARY KEY (user_id, statistic_time) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_user_custom_amount_statistics_data',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
);

-- 4. 定义渠道维度统计 Sink 表 (PolarDB/MySQL)
CREATE TABLE sink_channel_custom_amount_statistic (
  channel_id BIGINT,
  statistic_time DATE,
  user_count BIGINT,
  custom_increase_count BIGINT,
  custom_reduction_count BIGINT,
  custom_increase_amount DECIMAL(20, 4),
  custom_increase_withdraw_amount DECIMAL(20, 4),
  custom_increase_deposit_amount DECIMAL(20, 4),
  custom_increase_bonus_amount DECIMAL(20, 4),
  custom_reduction_amount DECIMAL(20, 4),
  custom_reduction_withdraw_amount DECIMAL(20, 4),
  custom_reduction_deposit_amount DECIMAL(20, 4),
  custom_reduction_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_channel_custom_amount_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
);

-- 5. 插入用户统计表
INSERT INTO sink_user_custom_amount_statistics
SELECT
  user_id,
  channel_id,
  statistic_time,
  COUNT(*) FILTER (WHERE `type` = 'INCREASE') AS custom_increase_count,
  COUNT(*) FILTER (WHERE `type` = 'REDUCE') AS custom_reduction_count,
  SUM(IF (`type` = 'INCREASE', amount, 0)) AS custom_increase_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'WITHDRAW', amount, 0)) AS custom_increase_withdraw_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'DEPOSIT', amount, 0)) AS custom_increase_deposit_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'BONUS', amount, 0)) AS custom_increase_bonus_amount,
  SUM(IF (`type` = 'REDUCE', amount, 0)) AS custom_reduction_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'WITHDRAW', amount, 0)) AS custom_reduction_withdraw_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'DEPOSIT', amount, 0)) AS custom_reduction_deposit_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'BONUS', amount, 0)) AS custom_reduction_bonus_amount,
  MIN(create_time) AS create_time,
  MAX(update_time) AS update_time
FROM dwd_amount_operation_record
GROUP BY
  user_id,
  channel_id,
  statistic_time;

-- 6. 插入渠道统计表
INSERT INTO sink_channel_custom_amount_statistic
SELECT
  channel_id,
  statistic_time,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(*) FILTER (WHERE `type` = 'INCREASE') AS custom_increase_count,
  COUNT(*) FILTER (WHERE `type` = 'REDUCE') AS custom_reduction_count,
  SUM(IF (`type` = 'INCREASE', amount, 0)) AS custom_increase_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'WITHDRAW', amount, 0)) AS custom_increase_withdraw_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'DEPOSIT', amount, 0)) AS custom_increase_deposit_amount,
  SUM(IF (`type` = 'INCREASE' AND amount_type = 'BONUS', amount, 0)) AS custom_increase_bonus_amount,
  SUM(IF (`type` = 'REDUCE', amount, 0)) AS custom_reduction_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'WITHDRAW', amount, 0)) AS custom_reduction_withdraw_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'DEPOSIT', amount, 0)) AS custom_reduction_deposit_amount,
  SUM(IF (`type` = 'REDUCE' AND amount_type = 'BONUS', amount, 0)) AS custom_reduction_bonus_amount
FROM dwd_amount_operation_record
GROUP BY
  channel_id,
  statistic_time;
