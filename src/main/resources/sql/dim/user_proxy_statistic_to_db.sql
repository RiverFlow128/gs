-- ==========================================
-- DIM层：代理统计维度表同步
-- 职责：从MySQLCDC读取代理统计数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_proxy_statistic (业务库)
--       ↓ CDC [本文件]
--   DIM: user_proxy_statistic (PolarDB statics库)
--
-- 上游表：MySQL.user_proxy_statistic - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_proxy_statistic (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 1. 核心配置
SET 'execution.checkpointing.interval' = '3min';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- 2. 定义MySQL CDC源表
CREATE TABLE source_user_proxy_statistic (
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
  `update_time` TIMESTAMP(6),
  PRIMARY KEY (`id`) NOT ENFORCED
)
WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '${MYSQL_HOST}',
  'port' = '${MYSQL_PORT}',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'database-name' = 'assetscenter-test',
  'table-name' = 'user_proxy_statistic',
  'server-id' = '420000-430000',
  'scan.incremental.snapshot.enabled' = 'true',
  'scan.startup.mode' = 'initial',
  'server-time-zone' = 'Asia/Bangkok'
);

-- 3. 定义PolarDB目标表
CREATE TABLE polar_user_proxy_statistic (
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
  `update_time` TIMESTAMP(6),
  PRIMARY KEY (`id`) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'user_proxy_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.parallelism' = '2',
  'connection.max-retry-timeout' = '60s'
);

-- 4. 数据同步
INSERT INTO polar_user_proxy_statistic
SELECT * FROM source_user_proxy_statistic;
