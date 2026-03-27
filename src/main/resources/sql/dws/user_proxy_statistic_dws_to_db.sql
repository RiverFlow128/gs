-- ==========================================
-- DWS 层：渠道代理统计聚合
-- 职责：从 DWD 层读取代理数据，按渠道聚合后写入 PolarDB
--
-- 数据血缘：
--   DWD: dwd_user_proxy_statistic (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: today_channel_proxy_statistic (PolarDB)
--
-- 上游表：dwd_user_proxy_statistic (Kafka)
-- 上游文件：dwd/user_proxy_statistic_dwd.sql
-- 下游表：today_channel_proxy_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- 1. 核心配置
SET 'table.exec.mini-batch.enabled' = 'true';

-- 允许 1 分钟的延迟，这意味着 Flink 会在内存中暂存 1 分钟的数据再写入 PolarDB，达到降本增效的
-- 减少 mini-batch 延迟，避免与 checkpoint 间隔冲突，防止数据积压
SET 'table.exec.mini-batch.allow-latency' = '1min';

-- 设置模式为 EXACTLY_ONCE
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 确保 Checkpoint 在任务取消时保留，以便重启恢复
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- 批处理的最大行数
SET 'table.exec.mini-batch.size' = '10000';

-- 开启状态清理，确保内存不会被历史数据撑爆（针对天级报表，设置 36 小时足够）
SET 'table.exec.state.ttl' = '36h';

-- 开启局部聚合优化，解决高基数下的 COUNT DISTINCT 性能问题
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 原配置（仅用于开发）
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 修改为：使用 OSS 分布式存储
SET 'execution.checkpointing.interval' = '3min';

-- 定义DWD层源表(从DWD层Kafka读取清洗后的数据)
CREATE TABLE dwd_user_proxy_statistic (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
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
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(6),
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_proxy_statistic',
  'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
  'properties.group.id' = 'dws_user_proxy_statistic_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

CREATE TABLE sink_today_channel_proxy_statistic (
  `channel_id` BIGINT,
  `statistic_time` DATE,
  `direct_bet_rebate_amount` DECIMAL(20, 4),
  `direct_recharge_rebate_amount` DECIMAL(20, 4),
  `teams_bet_rebate_amount` DECIMAL(20, 4),
  `teams_recharge_rebate_amount` DECIMAL(20, 4),
  `invitation_amount` DECIMAL(20, 4),
  `achievement_amount` DECIMAL(20, 4),
  `total_effective_bet_amount` DECIMAL(20, 4),
  `commissions_number` BIGINT,
  `valid_direct_member` BIGINT,
  `betting_rebate_count` BIGINT,
  `recharge_rebate_count` BIGINT,
  `betting_rebate_number` BIGINT,
  `recharge_rebate_number` BIGINT,
  `rebates_number` BIGINT,
  `total_transfer_amount` DECIMAL(20, 4),
  PRIMARY KEY (`statistic_time`, `channel_id`) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_proxy_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 1. 开启缓冲写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
    -- TODO: 修改原因 - JDBC Sink 默认 At-Least-Once，无法保证 Exactly-Once 语义
    -- 解决方案：在 INSERT 语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

  );

INSERT INTO
  sink_today_channel_proxy_statistic
SELECT
  channel_id,
  statistic_time,
  -- 金额累加
  SUM(direct_bet_rebate_amount) AS direct_bet_rebate_amount,
  SUM(direct_recharge_rebate_amount) AS direct_recharge_rebate_amount,
  SUM(teams_bet_rebate_amount) AS teams_bet_rebate_amount,
  SUM(teams_recharge_rebate_amount) AS teams_recharge_rebate_amount,
  SUM(invitation_amount) AS invitation_amount,
  SUM(achievement_amount) AS achievement_amount,
  SUM(effective_bet_amount) AS total_effective_bet_amount,
  -- 被返佣总人数：四项佣金之和不为 0 的人数去重统计 (Flink SQL 中 COUNT 作用于具体值时会自动忽略 NULL)
  COUNT(
    CASE
      WHEN (
        direct_bet_rebate_amount + direct_recharge_rebate_amount + teams_bet_rebate_amount + teams_recharge_rebate_amount
      ) <> 0 THEN user_id
      ELSE NULL
    END
  ) AS commissions_number,
  SUM(valid_direct_member) AS valid_direct_member,
  -- 次数累加
  CAST(SUM(betting_rebate_count) AS BIGINT) AS betting_rebate_count,
  CAST(SUM(recharge_rebate_count) AS BIGINT) AS recharge_rebate_count,
  -- 投注返佣人数：次数不为 0 的人数
  COUNT(
    CASE
      WHEN betting_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS betting_rebate_number,
  -- 充值返佣人数：次数不为 0 的人数
  COUNT(
    CASE
      WHEN recharge_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS recharge_rebate_number,
  -- 返佣总人数：投注或充值返佣次数不为 0 的人数
  COUNT(
    CASE
      WHEN betting_rebate_count <> 0
      OR recharge_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS rebates_number,
  SUM(transfer_amount) AS total_transfer_amount
FROM
  dwd_user_proxy_statistic
GROUP BY
  channel_id,
  statistic_time;
