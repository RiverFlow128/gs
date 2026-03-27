-- ==========================================
-- DIM层：代理账户维度表同步
-- 职责：从MySQLCDC读取代理账户数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_proxy_account (业务库)
--       ↓ CDC [本文件]
--   DIM: user_proxy_account (PolarDB statics库)
--
-- 上游表：MySQL.user_proxy_account - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_proxy_account (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 1. 核心配置
SET 'execution.checkpointing.interval' = '3min';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- 2. 定义MySQL CDC源表
CREATE TABLE source_user_proxy_account (
  `id` BIGINT,
  `user_id` BIGINT,
  `channel_id` BIGINT,
  `account_status` STRING,
  `extracted_amount` DECIMAL(20, 4),
  `withdrawal_amount` DECIMAL(20, 4),
  `pending_settlement_amount` DECIMAL(20, 4),
  `user_proxy_level` BIGINT,
  `total_user_proxy_effective_bet_amount` DECIMAL(20, 4),
  `total_transfer_amount` DECIMAL(20, 4),
  `total_effective_bet_amount` DECIMAL(20, 4),
  `total_team_effective_bet_amount` DECIMAL(20, 4),
  `total_direct_effective_bet_amount` DECIMAL(20, 4),
  `total_direct_bet_rebate_amount` DECIMAL(20, 4),
  `total_direct_recharge_rebate_amount` DECIMAL(20, 4),
  `total_teams_bet_rebate_amount` DECIMAL(20, 4),
  `total_teams_recharge_rebate_amount` DECIMAL(20, 4),
  `transfer_count` BIGINT,
  `betting_rebate_count` BIGINT,
  `recharge_rebate_count` BIGINT,
  `total_user_invitations_amount` DECIMAL(20, 4),
  `total_user_achievement_amount` DECIMAL(20, 4),
  `affiliation` BIGINT,
  `agent_activation_time` TIMESTAMP(3),
  `version` BIGINT,
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
  'database-name' = 'user_basic-test',
  'table-name' = 'user_proxy_account',
  'server-id' = '320000-330000',
  'scan.incremental.snapshot.enabled' = 'true',
  'scan.startup.mode' = 'initial',
  'server-time-zone' = 'Asia/Bangkok'
);

-- 3. 定义PolarDB目标表
CREATE TABLE polar_user_proxy_account (
  `id` BIGINT,
  `user_id` BIGINT,
  `channel_id` BIGINT,
  `account_status` STRING,
  `extracted_amount` DECIMAL(20, 4),
  `withdrawal_amount` DECIMAL(20, 4),
  `pending_settlement_amount` DECIMAL(20, 4),
  `user_proxy_level` BIGINT,
  `total_user_proxy_effective_bet_amount` DECIMAL(20, 4),
  `total_transfer_amount` DECIMAL(20, 4),
  `total_effective_bet_amount` DECIMAL(20, 4),
  `total_team_effective_bet_amount` DECIMAL(20, 4),
  `total_direct_effective_bet_amount` DECIMAL(20, 4),
  `total_direct_bet_rebate_amount` DECIMAL(20, 4),
  `total_direct_recharge_rebate_amount` DECIMAL(20, 4),
  `total_teams_bet_rebate_amount` DECIMAL(20, 4),
  `total_teams_recharge_rebate_amount` DECIMAL(20, 4),
  `transfer_count` BIGINT,
  `betting_rebate_count` BIGINT,
  `recharge_rebate_count` BIGINT,
  `total_user_invitations_amount` DECIMAL(20, 4),
  `total_user_achievement_amount` DECIMAL(20, 4),
  `affiliation` BIGINT,
  `agent_activation_time` TIMESTAMP(3),
  `version` BIGINT,
  `create_time` TIMESTAMP(3),
  `update_time` TIMESTAMP(6),
  PRIMARY KEY (id, create_time) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'user_proxy_account',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '5s',
  'sink.parallelism' = '2',
  'connection.max-retry-timeout' = '60s'
);

-- 4. 数据同步
INSERT INTO polar_user_proxy_account
SELECT * FROM source_user_proxy_account;
