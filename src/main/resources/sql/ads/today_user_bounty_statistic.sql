-- ==========================================
-- ADS层：用户赏金约战统计表
-- 职责：从DWD层读取赏金数据，进行聚合统计后写入PolarDB
--
-- 数据血缘（待接入）：
--   DWD: dwd_bounty_record (Kafka, 待创建)
--       ↓ Flink SQL 聚合 [本文件]
--   ADS: today_user_bounty_statistic (PolarDB) - 日级统计
--        user_bounty_statistic (PolarDB) - 累计统计
--
-- 上游表：dwd_bounty_record (Kafka, 待接入)
-- 上游文件：dwd/bounty_record_dwd.sql (待创建)
-- 下游表：today_user_bounty_statistic, user_bounty_statistic (PolarDB)
-- 下游文件：无（应用层终端表）
--
-- 注意：目前为占位表，待赏金业务接入后完善
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

-- 2. 定义DWD层赏金数据源表 (从Kafka读取，待业务接入后启用)
-- TODO: 待赏金业务表接入后，替换为实际的Kafka topic和字段
CREATE TABLE dwd_bounty_record (
  `key` STRING,
  `dataSyncType` STRING,
  `jsonData` ROW<
    id BIGINT,
    user_id BIGINT,
    channel_id BIGINT,
    bounty_order_no STRING,
    bet_amount DECIMAL(20, 4),
    settle_amount DECIMAL(20, 4),
    win_loss_amount DECIMAL(20, 4),
    punish_amount DECIMAL(20, 4),
    pumped_amount DECIMAL(20, 4),
    profit_loss_amount DECIMAL(20, 4),
    bet_deposit_amount DECIMAL(20, 4),
    bet_withdrawal_amount DECIMAL(20, 4),
    bet_bonus_amount DECIMAL(20, 4),
    settle_deposit_amount DECIMAL(20, 4),
    settle_withdrawal_amount DECIMAL(20, 4),
    settle_bonus_amount DECIMAL(20, 4),
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3)
  >,
  PRIMARY KEY (`key`) NOT ENFORCED
)
WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_bounty_record',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'ads_bounty_statistic_group',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- ==========================================
-- 3. 日级统计表（按天汇总，有statistic_time字段）
-- ==========================================

-- 3.1 定义日级统计目标表
CREATE TABLE ads_today_user_bounty_statistic (
  statistic_time DATE COMMENT '统计日期',
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 有效投注统计
  bounty_total_effective_bet_count INT COMMENT '赏金约战有效投注单数',
  bounty_total_effective_bet_amount DECIMAL(20, 4) COMMENT '赏金有效投注金额',
  bounty_total_effective_bet_deposit_amount DECIMAL(20, 4) COMMENT '赏金有效投注锁定余额金额',
  bounty_total_effective_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金有效投注可提现余额金额',
  bounty_total_effective_bet_bonus_amount DECIMAL(20, 4) COMMENT '赏金有效投注奖金金额',
  
  -- 投注统计
  bounty_total_bet_amount DECIMAL(20, 4) COMMENT '赏金投注总金额',
  bounty_total_bet_count INT COMMENT '赏金投注总单数',
  bounty_total_bet_deposit_amount DECIMAL(20, 4) COMMENT '赏金投注总锁定余额金额',
  bounty_total_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金投注总可提现余额金额',
  bounty_total_bet_bonus_amount DECIMAL(20, 4) COMMENT '赏金投注总奖金金额',
  
  -- 惩罚统计
  bounty_total_punish_amount DECIMAL(20, 4) COMMENT '赏金保证金惩罚',
  bounty_total_punish_count DECIMAL(20, 4) COMMENT '赏金保证金惩罚次数',
  bounty_total_punish_deposit_amount DECIMAL(20, 4) COMMENT '赏金保证金锁定余额惩罚',
  bounty_total_punish_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金保证金可提现余额惩罚',
  bounty_total_punish_bonus_amount DECIMAL(20, 4) COMMENT '赏金保证金奖金惩罚',
  
  -- 抽水统计
  bounty_total_pumped_amount DECIMAL(20, 4) COMMENT '赏金抽水金额',
  bounty_total_pumped_count DECIMAL(20, 4) COMMENT '赏金抽水次数',
  bounty_total_pumped_deposit_amount DECIMAL(20, 4) COMMENT '赏金抽水锁定余额金额',
  bounty_total_pumped_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金抽水可提现余额金额',
  bounty_total_pumped_bonus_amount DECIMAL(20, 4) COMMENT '赏金抽水奖金金额',
  
  -- 盈亏统计
  bounty_total_profit_loss_amount DECIMAL(20, 4) COMMENT '赏金总盈亏 (赏金输赢-保证金惩罚金额)',
  bounty_total_profit_loss_count DECIMAL(20, 4) COMMENT '赏金盈亏次数',
  bounty_total_profit_loss_deposit_amount DECIMAL(20, 4) COMMENT '赏金总盈亏锁定余额',
  bounty_total_profit_loss_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金总盈亏可提现余额',
  bounty_total_profit_loss_bonus_amount DECIMAL(20, 4) COMMENT '赏金总盈亏奖金',
  
  -- 输赢统计
  bounty_total_win_loss_amount DECIMAL(20, 4) COMMENT '赏金输赢金额 (结算金额-投注金额)',
  bounty_total_win_loss_count DECIMAL(20, 4) COMMENT '赏金输赢次数',
  bounty_total_win_loss_deposit_amount DECIMAL(20, 4) COMMENT '赏金输赢锁定余额金额',
  bounty_total_win_loss_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金输赢可提现余额金额',
  bounty_total_win_loss_bonus_amount DECIMAL(20, 4) COMMENT '赏金输赢奖金金额',
  
  -- 结算统计
  bounty_total_settle_amount DECIMAL(20, 4) COMMENT '赏金结算金额',
  bounty_total_settle_count DECIMAL(20, 4) COMMENT '赏金结算次数',
  bounty_total_settle_deposit_amount DECIMAL(20, 4) COMMENT '赏金结算锁定余额金额',
  bounty_total_settle_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金结算可提现余额金额',
  bounty_total_settle_bonus_amount DECIMAL(20, 4) COMMENT '赏金结算奖金金额',
  
  PRIMARY KEY (user_id, channel_id, statistic_time) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_user_bounty_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 3.2 日级统计聚合逻辑（目前输出全0，待业务接入后完善）
-- TODO: 待赏金业务数据接入后，替换为实际的聚合逻辑
INSERT INTO ads_today_user_bounty_statistic
SELECT
  CAST(jsonData.update_time AS DATE) AS statistic_time,
  jsonData.user_id,
  jsonData.channel_id,
  
  -- 有效投注统计
  0 AS bounty_total_effective_bet_count,
  0 AS bounty_total_effective_bet_amount,
  0 AS bounty_total_effective_bet_deposit_amount,
  0 AS bounty_total_effective_bet_withdrawal_amount,
  0 AS bounty_total_effective_bet_bonus_amount,
  
  -- 投注统计
  0 AS bounty_total_bet_amount,
  0 AS bounty_total_bet_count,
  0 AS bounty_total_bet_deposit_amount,
  0 AS bounty_total_bet_withdrawal_amount,
  0 AS bounty_total_bet_bonus_amount,
  
  -- 惩罚统计
  0 AS bounty_total_punish_amount,
  0 AS bounty_total_punish_count,
  0 AS bounty_total_punish_deposit_amount,
  0 AS bounty_total_punish_withdrawal_amount,
  0 AS bounty_total_punish_bonus_amount,
  
  -- 抽水统计
  0 AS bounty_total_pumped_amount,
  0 AS bounty_total_pumped_count,
  0 AS bounty_total_pumped_deposit_amount,
  0 AS bounty_total_pumped_withdrawal_amount,
  0 AS bounty_total_pumped_bonus_amount,
  
  -- 盈亏统计
  0 AS bounty_total_profit_loss_amount,
  0 AS bounty_total_profit_loss_count,
  0 AS bounty_total_profit_loss_deposit_amount,
  0 AS bounty_total_profit_loss_withdrawal_amount,
  0 AS bounty_total_profit_loss_bonus_amount,
  
  -- 输赢统计
  0 AS bounty_total_win_loss_amount,
  0 AS bounty_total_win_loss_count,
  0 AS bounty_total_win_loss_deposit_amount,
  0 AS bounty_total_win_loss_withdrawal_amount,
  0 AS bounty_total_win_loss_bonus_amount,
  
  -- 结算统计
  0 AS bounty_total_settle_amount,
  0 AS bounty_total_settle_count,
  0 AS bounty_total_settle_deposit_amount,
  0 AS bounty_total_settle_withdrawal_amount,
  0 AS bounty_total_settle_bonus_amount

FROM dwd_bounty_record
WHERE jsonData.update_time IS NOT NULL
  AND 1 = 0  -- 暂时不输出数据，待业务接入后删除此行
GROUP BY 
  CAST(jsonData.update_time AS DATE),
  jsonData.user_id,
  jsonData.channel_id;

-- ==========================================
-- 5. 累计统计表（全历史汇总，不分日期）
-- ==========================================

-- 5.1 定义累计统计目标表
CREATE TABLE ads_user_bounty_statistic (
  user_id BIGINT COMMENT '用户ID',
  channel_id BIGINT COMMENT '渠道ID',
  
  -- 有效投注统计
  bounty_total_effective_bet_count INT COMMENT '赏金约战有效投注单数',
  bounty_total_effective_bet_amount DECIMAL(20, 4) COMMENT '赏金有效投注金额',
  bounty_total_effective_bet_deposit_amount DECIMAL(20, 4) COMMENT '赏金有效投注锁定余额金额',
  bounty_total_effective_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金有效投注可提现余额金额',
  bounty_total_effective_bet_bonus_amount DECIMAL(20, 4) COMMENT '赏金有效投注奖金金额',
  
  -- 投注统计
  bounty_total_bet_amount DECIMAL(20, 4) COMMENT '赏金投注总金额',
  bounty_total_bet_count INT COMMENT '赏金投注总单数',
  bounty_total_bet_deposit_amount DECIMAL(20, 4) COMMENT '赏金投注总锁定余额金额',
  bounty_total_bet_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金投注总可提现余额金额',
  bounty_total_bet_bonus_amount DECIMAL(20, 4) COMMENT '赏金投注总奖金金额',
  
  -- 惩罚统计
  bounty_total_punish_amount DECIMAL(20, 4) COMMENT '赏金保证金惩罚',
  bounty_total_punish_count DECIMAL(20, 4) COMMENT '赏金保证金惩罚次数',
  bounty_total_punish_deposit_amount DECIMAL(20, 4) COMMENT '赏金保证金锁定余额惩罚',
  bounty_total_punish_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金保证金可提现余额惩罚',
  bounty_total_punish_bonus_amount DECIMAL(20, 4) COMMENT '赏金保证金奖金惩罚',
  
  -- 抽水统计
  bounty_total_pumped_amount DECIMAL(20, 4) COMMENT '赏金抽水金额',
  bounty_total_pumped_count DECIMAL(20, 4) COMMENT '赏金抽水次数',
  bounty_total_pumped_deposit_amount DECIMAL(20, 4) COMMENT '赏金抽水锁定余额金额',
  bounty_total_pumped_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金抽水可提现余额金额',
  bounty_total_pumped_bonus_amount DECIMAL(20, 4) COMMENT '赏金抽水奖金金额',
  
  -- 盈亏统计
  bounty_total_profit_loss_amount DECIMAL(20, 4) COMMENT '赏金总盈亏',
  bounty_total_profit_loss_count DECIMAL(20, 4) COMMENT '赏金盈亏次数',
  bounty_total_profit_loss_deposit_amount DECIMAL(20, 4) COMMENT '赏金总盈亏锁定余额',
  bounty_total_profit_loss_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金总盈亏可提现余额',
  bounty_total_profit_loss_bonus_amount DECIMAL(20, 4) COMMENT '赏金总盈亏奖金',
  
  -- 输赢统计
  bounty_total_win_loss_amount DECIMAL(20, 4) COMMENT '赏金输赢金额',
  bounty_total_win_loss_count DECIMAL(20, 4) COMMENT '赏金输财次数',
  bounty_total_win_loss_deposit_amount DECIMAL(20, 4) COMMENT '赏金输赢锁定余额金额',
  bounty_total_win_loss_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金输赢可提现余额金额',
  bounty_total_win_loss_bonus_amount DECIMAL(20, 4) COMMENT '赏金输赢奖金金额',
  
  -- 结算统计
  bounty_total_settle_amount DECIMAL(20, 4) COMMENT '赏金结算金额',
  bounty_total_settle_count DECIMAL(20, 4) COMMENT '赏金结算次数',
  bounty_total_settle_deposit_amount DECIMAL(20, 4) COMMENT '赏金结算锁定余额金额',
  bounty_total_settle_withdrawal_amount DECIMAL(20, 4) COMMENT '赏金结算可提现余额金额',
  bounty_total_settle_bonus_amount DECIMAL(20, 4) COMMENT '赏金结算奖金金额',
  
  PRIMARY KEY (user_id, channel_id) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'user_bounty_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.max-retries' = '3'
);

-- 5.2 累计统计聚合逻辑（目前输出全0，待业务接入后完善）
INSERT INTO ads_user_bounty_statistic
SELECT
  jsonData.user_id,
  jsonData.channel_id,
  
  -- 有效投注统计
  0 AS bounty_total_effective_bet_count,
  0 AS bounty_total_effective_bet_amount,
  0 AS bounty_total_effective_bet_deposit_amount,
  0 AS bounty_total_effective_bet_withdrawal_amount,
  0 AS bounty_total_effective_bet_bonus_amount,
  
  -- 投注统计
  0 AS bounty_total_bet_amount,
  0 AS bounty_total_bet_count,
  0 AS bounty_total_bet_deposit_amount,
  0 AS bounty_total_bet_withdrawal_amount,
  0 AS bounty_total_bet_bonus_amount,
  
  -- 惩罚统计
  0 AS bounty_total_punish_amount,
  0 AS bounty_total_punish_count,
  0 AS bounty_total_punish_deposit_amount,
  0 AS bounty_total_punish_withdrawal_amount,
  0 AS bounty_total_punish_bonus_amount,
  
  -- 抽水统计
  0 AS bounty_total_pumped_amount,
  0 AS bounty_total_pumped_count,
  0 AS bounty_total_pumped_deposit_amount,
  0 AS bounty_total_pumped_withdrawal_amount,
  0 AS bounty_total_pumped_bonus_amount,
  
  -- 盈亏统计
  0 AS bounty_total_profit_loss_amount,
  0 AS bounty_total_profit_loss_count,
  0 AS bounty_total_profit_loss_deposit_amount,
  0 AS bounty_total_profit_loss_withdrawal_amount,
  0 AS bounty_total_profit_loss_bonus_amount,
  
  -- 输赢统计
  0 AS bounty_total_win_loss_amount,
  0 AS bounty_total_win_loss_count,
  0 AS bounty_total_win_loss_deposit_amount,
  0 AS bounty_total_win_loss_withdrawal_amount,
  0 AS bounty_total_win_loss_bonus_amount,
  
  -- 结算统计
  0 AS bounty_total_settle_amount,
  0 AS bounty_total_settle_count,
  0 AS bounty_total_settle_deposit_amount,
  0 AS bounty_total_settle_withdrawal_amount,
  0 AS bounty_total_settle_bonus_amount

FROM dwd_bounty_record
WHERE jsonData.update_time IS NOT NULL
  AND 1 = 0  -- 暂时不输出数据，待业务接入后删除此行
GROUP BY 
  jsonData.user_id,
  jsonData.channel_id;
