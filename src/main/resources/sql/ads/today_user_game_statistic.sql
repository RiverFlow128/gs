-- ==========================================
-- ADS层：用户游戏统计表
-- 职责：从DWS层读取游戏数据，进行聚合统计后写入PolarDB
-- 
-- 数据血缘：
--   DWS: today_user_game_statistics_dws (PolarDB)
--       ↓ CDC (mysql-cdc) [本文件]
--   ADS: today_user_game_statistic (PolarDB) - 日级统计
--        user_game_statistic (PolarDB) - 累计统计
--
-- 上游表：today_user_game_statistics_dws (PolarDB)
-- 上游文件：dws/kafka_to_game.sql
-- 下游表：today_user_game_statistic, user_game_statistic (PolarDB)
-- 下游文件：无（应用层终端表）
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

-- 2. 定义DWS层游戏数据源表 (从PolarDB CDC读取)
CREATE TABLE dws_user_game_statistics (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  third_platform_provider_type STRING,
  bet_count BIGINT,
  effective_bet_count BIGINT,
  settle_count BIGINT,
  settle_bet_amount DECIMAL(20, 4),
  bet_amount DECIMAL(20, 4),
  settle_amount DECIMAL(20, 4),
  win_lose_amount DECIMAL(20, 4),
  effective_bet_amount DECIMAL(20, 4),
  bet_deposit_amount DECIMAL(20, 4),
  bet_withdrawal_amount DECIMAL(20, 4),
  bet_bonus_amount DECIMAL(20, 4),
  settle_deposit_amount DECIMAL(20, 4),
  settle_withdrawal_amount DECIMAL(20, 4),
  settle_bonus_amount DECIMAL(20, 4),
  win_lose_deposit_amount DECIMAL(20, 4),
  win_lose_withdrawal_amount DECIMAL(20, 4),
  win_lose_bonus_amount DECIMAL(20, 4),
  effective_bet_deposit_amount DECIMAL(20, 4),
  effective_bet_withdrawal_amount DECIMAL(20, 4),
  effective_bet_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (
    statistic_time,
    user_id,
    game_id,
    game_category_id,
    channel_id,
    third_platform_provider_type
  ) NOT ENFORCED
)
WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '${MYSQL_HOST}',
  'port' = '1001',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'database-name' = 'statics',
  'table-name' = 'today_user_game_statistics_dws',
  'server-id' = '7001-8000',
  'server-time-zone' = 'Asia/Bangkok',
  'scan.startup.mode' = 'latest-offset'
);

-- ==========================================
-- 3. 日级统计表（按天汇总，有statistic_time字段）
-- ==========================================

-- 3.1 定义日级统计目标表
CREATE TABLE ads_today_user_game_statistic (
  statistic_time DATE COMMENT '统计日期',
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 总投注概览
  total_effective_bet_amount DECIMAL(20, 4) COMMENT '总有效投注总额',
  total_effective_bet_count INT COMMENT '总有效投注单数',
  
  -- 三方游戏明细
  third_party_total_effective_bet_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注金额',
  third_party_total_effective_bet_count INT COMMENT '三方游戏有效投注单数',
  third_party_total_effective_bet_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注锁定金额',
  third_party_total_effective_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注提现金额',
  third_party_total_effective_bet_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注奖金金额',
  
  -- 输赢统计
  third_party_total_win_loss_amount DECIMAL(20, 4) COMMENT '三方游戏输赢（转账金额，公式：结算金额-投注金额）',
  third_party_total_win_loss_withdraw_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-可提现',
  third_party_total_win_loss_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-锁定余额',
  third_party_total_win_loss_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-奖金',
  
  -- 杀率
  third_party_total_kill_rate DECIMAL(20, 4) COMMENT '三方游戏杀率 杀率=-输赢金额/已结算的投注总额',
  
  -- 投注统计
  third_party_total_settle_bet_amount DECIMAL(20, 4) COMMENT '三方游戏投注金额（已结算）',
  third_party_total_bet_amount DECIMAL(20, 4) COMMENT '三方游戏投注金额',
  third_party_total_bet_count INT COMMENT '三方游戏投注单数',
  third_party_total_bet_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏投注锁定金额',
  third_party_total_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏投注可提现金额',
  third_party_total_bet_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏投注奖金金额',
  
  -- 结算统计
  third_party_total_settle_amount DECIMAL(20, 4) COMMENT '三方游戏结算金额',
  third_party_total_settle_count DECIMAL(20, 4) COMMENT '三方游戏结算次数',
  third_party_total_settle_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏结算锁定金额',
  third_party_total_settle_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏结算提现金额',
  third_party_total_settle_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏结算奖金金额',
  
  PRIMARY KEY (user_id, channel_id, statistic_time) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_user_game_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 3.2 日级统计聚合逻辑
INSERT INTO ads_today_user_game_statistic
SELECT
  statistic_time,
  user_id,
  channel_id,
  
  -- 总投注概览
  SUM(effective_bet_amount) AS total_effective_bet_amount,
  SUM(CAST(effective_bet_count AS INT)) AS total_effective_bet_count,
  
  -- 三方游戏明细
  SUM(effective_bet_amount) AS third_party_total_effective_bet_amount,
  SUM(CAST(effective_bet_count AS INT)) AS third_party_total_effective_bet_count,
  SUM(effective_bet_deposit_amount) AS third_party_total_effective_bet_deposit_amount,
  SUM(effective_bet_withdrawal_amount) AS third_party_total_effective_bet_withdrawal_amount,
  SUM(effective_bet_bonus_amount) AS third_party_total_effective_bet_bonus_amount,
  
  -- 输赢统计
  SUM(win_lose_amount) AS third_party_total_win_loss_amount,
  SUM(win_lose_withdrawal_amount) AS third_party_total_win_loss_withdraw_amount,
  SUM(win_lose_deposit_amount) AS third_party_total_win_loss_deposit_amount,
  SUM(win_lose_bonus_amount) AS third_party_total_win_loss_bonus_amount,
  
  -- 杀率计算
  CASE 
    WHEN SUM(settle_bet_amount) = 0 THEN 0 
    ELSE -SUM(win_lose_amount) / SUM(settle_bet_amount) 
  END AS third_party_total_kill_rate,
  
  -- 投注统计
  SUM(settle_bet_amount) AS third_party_total_settle_bet_amount,
  SUM(bet_amount) AS third_party_total_bet_amount,
  SUM(CAST(bet_count AS INT)) AS third_party_total_bet_count,
  SUM(bet_deposit_amount) AS third_party_total_bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS third_party_total_bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS third_party_total_bet_bonus_amount,
  
  -- 结算统计
  SUM(settle_amount) AS third_party_total_settle_amount,
  SUM(CAST(settle_count AS DECIMAL(20, 4))) AS third_party_total_settle_count,
  SUM(settle_deposit_amount) AS third_party_total_settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS third_party_total_settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS third_party_total_settle_bonus_amount

FROM dws_user_game_statistics
GROUP BY 
  statistic_time,
  user_id,
  channel_id;

-- ==========================================
-- 5. 累计统计表（全历史汇总，不分日期）
-- ==========================================

-- 5.1 定义累计统计目标表
CREATE TABLE ads_user_game_statistic (
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 总投注概览
  total_effective_bet_amount DECIMAL(20, 4) COMMENT '总有效投注总额',
  total_effective_bet_count INT COMMENT '总有效投注单数',
  
  -- 三方游戏明细
  third_party_total_effective_bet_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注金额',
  third_party_total_effective_bet_count INT COMMENT '三方游戏有效投注单数',
  third_party_total_effective_bet_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注锁定金额',
  third_party_total_effective_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注提现金额',
  third_party_total_effective_bet_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏有效投注奖金金额',
  
  -- 输赢统计
  third_party_total_win_loss_amount DECIMAL(20, 4) COMMENT '三方游戏输赢',
  third_party_total_win_loss_withdraw_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-可提现',
  third_party_total_win_loss_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-锁定余额',
  third_party_total_win_loss_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏输赢-奖金',
  
  -- 杀率
  third_party_total_kill_rate DECIMAL(20, 4) COMMENT '三方游戏杀率',
  
  -- 投注统计
  third_party_total_settle_bet_amount DECIMAL(20, 4) COMMENT '三方游戏投注金额（已结算）',
  third_party_total_bet_amount DECIMAL(20, 4) COMMENT '三方游戏投注金额',
  third_party_total_bet_count INT COMMENT '三方游戏投注单数',
  third_party_total_bet_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏投注锁定金额',
  third_party_total_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏投注可提现金额',
  third_party_total_bet_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏投注奖金金额',
  
  -- 结算统计
  third_party_total_settle_amount DECIMAL(20, 4) COMMENT '三方游戏结算金额',
  third_party_total_settle_count DECIMAL(20, 4) COMMENT '三方游戏结算次数',
  third_party_total_settle_deposit_amount DECIMAL(20, 4) COMMENT '三方游戏结算锁定金额',
  third_party_total_settle_withdrawal_amount DECIMAL(20, 4) COMMENT '三方游戏结算提现金额',
  third_party_total_settle_bonus_amount DECIMAL(20, 4) COMMENT '三方游戏结算奖金金额',
  
  PRIMARY KEY (user_id, channel_id) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'user_game_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 5.2 累计统计聚合逻辑（全历史汇总，不分日期）
INSERT INTO ads_user_game_statistic
SELECT
  user_id,
  channel_id,
  
  -- 总投注概览
  SUM(effective_bet_amount) AS total_effective_bet_amount,
  SUM(CAST(effective_bet_count AS INT)) AS total_effective_bet_count,
  
  -- 三方游戏明细
  SUM(effective_bet_amount) AS third_party_total_effective_bet_amount,
  SUM(CAST(effective_bet_count AS INT)) AS third_party_total_effective_bet_count,
  SUM(effective_bet_deposit_amount) AS third_party_total_effective_bet_deposit_amount,
  SUM(effective_bet_withdrawal_amount) AS third_party_total_effective_bet_withdrawal_amount,
  SUM(effective_bet_bonus_amount) AS third_party_total_effective_bet_bonus_amount,
  
  -- 输赢统计
  SUM(win_lose_amount) AS third_party_total_win_loss_amount,
  SUM(win_lose_withdrawal_amount) AS third_party_total_win_loss_withdraw_amount,
  SUM(win_lose_deposit_amount) AS third_party_total_win_loss_deposit_amount,
  SUM(win_lose_bonus_amount) AS third_party_total_win_loss_bonus_amount,
  
  -- 杀率计算
  CASE 
    WHEN SUM(settle_bet_amount) = 0 THEN 0 
    ELSE -SUM(win_lose_amount) / SUM(settle_bet_amount) 
  END AS third_party_total_kill_rate,
  
  -- 投注统计
  SUM(settle_bet_amount) AS third_party_total_settle_bet_amount,
  SUM(bet_amount) AS third_party_total_bet_amount,
  SUM(CAST(bet_count AS INT)) AS third_party_total_bet_count,
  SUM(bet_deposit_amount) AS third_party_total_bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS third_party_total_bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS third_party_total_bet_bonus_amount,
  
  -- 结算统计
  SUM(settle_amount) AS third_party_total_settle_amount,
  SUM(CAST(settle_count AS DECIMAL(20, 4))) AS third_party_total_settle_count,
  SUM(settle_deposit_amount) AS third_party_total_settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS third_party_total_settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS third_party_total_settle_bonus_amount

FROM dws_user_game_statistics
GROUP BY 
  user_id,
  channel_id;
