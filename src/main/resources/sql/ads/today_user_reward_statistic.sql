-- ==========================================
-- ADS层：用户奖励统计表
-- 职责：从DWD层读取奖励数据，进行聚合统计后写入PolarDB
-- 
-- 数据血缘：
--   DWD: dwd_reward_record (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   ADS: today_user_reward_statistic (PolarDB) - 日级统计
--        user_reward_statistic (PolarDB) - 累计统计
--
-- 上游表：dwd_reward_record (Kafka)
-- 上游文件：dwd/reward_record_dwd.sql
-- 下游表：today_user_reward_statistic, user_reward_statistic (PolarDB)
-- 下游文件：无（应用层终端表）
--
-- 注意：本表直接从DWD层聚合，与DWS层reward_record统计口径不同
-- ==========================================

-- 1. 核心配置
SET 'execution.checkpointing.interval' = '3min';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'execution.checkpointing.min-pause' = '1min';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3min';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.exec.state.ttl' = '36h';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.local-time-zone' = 'Asia/Singapore';

-- 2. 定义DWD层奖励数据源表 (从Kafka读取，debezium-json格式)
CREATE TABLE dwd_reward_record (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  unique_id BIGINT,
  reward_scope STRING,
  reward_type STRING,
  activity_type STRING,
  id BIGINT,
  bonus_amount DECIMAL(20, 4),
  draw_flag INT,
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
  'properties.group.id' = 'ads_reward_statistic_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- ==========================================
-- 3. 日级统计表（按天汇总，有statistic_time字段）
-- ==========================================

-- 3.1 定义日级统计目标表
CREATE TABLE ads_today_user_reward_statistic (
  statistic_time DATE COMMENT '统计日期',
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 总奖励统计
  total_reward_amount DECIMAL(20, 4) COMMENT '总奖励金额',
  total_reward_count INT COMMENT '总奖励次数',
  
  -- 按奖励类型统计（已领取）
  total_reward_withdrawal_amount DECIMAL(20, 4) COMMENT '总奖励可提现余额',
  total_reward_withdrawal_count INT COMMENT '总奖励可提现次数',
  total_reward_deposit_amount DECIMAL(20, 4) COMMENT '总奖励锁定余额',
  total_reward_deposit_count INT COMMENT '总奖励锁定次数',
  total_reward_bonus_amount DECIMAL(20, 4) COMMENT '总奖励奖金金额',
  total_reward_bonus_count INT COMMENT '总奖励奖金次数',
  
  -- 奖金任务统计
  total_reward_task_amount DECIMAL(20, 4) COMMENT '总奖金任务金额',
  total_reward_task_count INT COMMENT '总奖金任务次数',
  total_reward_task_draw_amount DECIMAL(20, 4) COMMENT '已完成并领取奖金任务金额',
  total_reward_task_draw_count INT COMMENT '已完成并领取奖金任务次数',
  
  PRIMARY KEY (user_id, channel_id, statistic_time) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_user_reward_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 3.2 日级统计聚合逻辑
INSERT INTO ads_today_user_reward_statistic
SELECT
  statistic_time,
  user_id,
  channel_id,
  
  -- 总奖励金额（已领取）
  SUM(CASE WHEN draw_flag = 1 THEN 
    CASE WHEN reward_scope = 'BONUS_TASK' THEN 
      CASE WHEN reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END 
    ELSE bonus_amount END 
  ELSE 0 END) AS total_reward_amount,
  
  SUM(CASE WHEN draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_count,
  
  -- 按奖励类型统计（已领取）
  SUM(CASE WHEN reward_type = 'WITHDRAW' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_withdrawal_amount,
  SUM(CASE WHEN reward_type = 'WITHDRAW' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_withdrawal_count,
  
  SUM(CASE WHEN reward_type = 'DEPOSIT' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_deposit_amount,
  SUM(CASE WHEN reward_type = 'DEPOSIT' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_deposit_count,
  
  SUM(CASE WHEN reward_type = 'BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_bonus_amount,
  SUM(CASE WHEN reward_type = 'BONUS' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_bonus_count,
  
  -- 奖金任务统计
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS total_reward_task_amount,
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 ELSE 0 END) AS total_reward_task_count,
  
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_task_draw_amount,
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_task_draw_count

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY 
  statistic_time,
  user_id,
  channel_id;

-- ==========================================
-- 5. 累计统计表（全历史汇总，不分日期）
-- ==========================================

-- 5.1 定义累计统计目标表
CREATE TABLE ads_user_reward_statistic (
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 总奖励统计
  total_reward_amount DECIMAL(20, 4) COMMENT '总奖励金额',
  total_reward_count INT COMMENT '总奖励次数',
  
  -- 按奖励类型统计（已领取）
  total_reward_withdrawal_amount DECIMAL(20, 4) COMMENT '总奖励可提现余额',
  total_reward_withdrawal_count INT COMMENT '总奖励可提现次数',
  total_reward_deposit_amount DECIMAL(20, 4) COMMENT '总奖励锁定余额',
  total_reward_deposit_count INT COMMENT '总奖励锁定次数',
  total_reward_bonus_amount DECIMAL(20, 4) COMMENT '总奖励奖金金额',
  total_reward_bonus_count INT COMMENT '总奖励奖金次数',
  
  -- 奖金任务统计
  total_reward_task_amount DECIMAL(20, 4) COMMENT '总奖金任务金额',
  total_reward_task_count INT COMMENT '总奖金任务次数',
  total_reward_task_draw_amount DECIMAL(20, 4) COMMENT '已完成并领取奖金任务金额',
  total_reward_task_draw_count INT COMMENT '已完成并领取奖金任务次数',
  
  PRIMARY KEY (user_id, channel_id) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'user_reward_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 5.2 累计统计聚合逻辑（全历史汇总，不分日期）
INSERT INTO ads_user_reward_statistic
SELECT
  user_id,
  channel_id,
  
  -- 总奖励金额（已领取）
  SUM(CASE WHEN draw_flag = 1 THEN 
    CASE WHEN reward_scope = 'BONUS_TASK' THEN 
      CASE WHEN reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END 
    ELSE bonus_amount END 
  ELSE 0 END) AS total_reward_amount,
  
  SUM(CASE WHEN draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_count,
  
  -- 按奖励类型统计（已领取）
  SUM(CASE WHEN reward_type = 'WITHDRAW' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_withdrawal_amount,
  SUM(CASE WHEN reward_type = 'WITHDRAW' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_withdrawal_count,
  
  SUM(CASE WHEN reward_type = 'DEPOSIT' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_deposit_amount,
  SUM(CASE WHEN reward_type = 'DEPOSIT' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_deposit_count,
  
  SUM(CASE WHEN reward_type = 'BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_bonus_amount,
  SUM(CASE WHEN reward_type = 'BONUS' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_bonus_count,
  
  -- 奖金任务统计
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS total_reward_task_amount,
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 ELSE 0 END) AS total_reward_task_count,
  
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS total_reward_task_draw_amount,
  SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 ELSE 0 END) AS total_reward_task_draw_count

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY 
  user_id,
  channel_id;
