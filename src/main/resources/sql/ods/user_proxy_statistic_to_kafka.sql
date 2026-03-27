-- ==========================================
-- ODS层：代理统计数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.user_proxy_statistic (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_user_proxy_statistic_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_user_proxy_statistic (Kafka)
--
-- 上游表：MySQL.user_proxy_statistic (CDC) - 业务库直连
-- 下游表：flink_user_proxy_statistic_data_sync (Kafka)
-- 下游文件：dwd/user_proxy_statistic_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 原配置（仅用于开发）
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 修改为：使用OSS分布式存储
-- 执行优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

-- 确保两个 CP 之间有间隔，减少存储压力
-- 1. 定义源表 (1024切片，只读取增量)
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
    -- Flink 内部状态去重依赖，通常建议使用物理表主键
    PRIMARY KEY (`id`) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'assetscenter-test',
    'table-name' = 'user_proxy_statistic',
    'server-id' = '7501-7520', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE sink_kafka (
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
WITH
  (
    'connector' = 'kafka',
    'topic' = 'flink_user_proxy_statistic_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    -- 使用 debezium-json 格式可以方便解析 MySQL CDC 的数据流
    'format' = 'debezium-json',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
  );

-- 3. 提交任务
INSERT INTO
  sink_kafka
SELECT
  *
FROM
  source_user_proxy_statistic;
