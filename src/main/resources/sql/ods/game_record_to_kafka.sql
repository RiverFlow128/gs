-- ==========================================
-- ODS层：游戏记录数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.game_record (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_game_order_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_game_record (Kafka)
--
-- 上游表：MySQL.game_record (CDC) - 业务库直连
-- 下游表：flink_game_order_data_sync (Kafka)
-- 下游文件：dwd/game_record_dwd.sql, dws/kafka_to_game.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- 容错测试方案：使用 HDFS 替代 NAS (NameNode 离线开启安全模式)
SET 'state.checkpoints.dir' = 'hdfs://hadoop101:8020/flink/checkpoints/game_record';
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- 性能优化配置
SET 'table.exec.resource.default-parallelism' = '2';

-- 默认并行度，根据 CPU 核心数调整
-- SET 'execution.checkpointing.min-pause' = '1min';

-- 确保两个 CP 之间有间隔，减少存储压力
-- 1. 定义源表 (1024分表 只读增量)
CREATE TABLE source_game_record (
  id BIGINT,
  user_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  bet_amount DECIMAL(19, 2),
  settle_amount DECIMAL(19, 2),
  transfer_amount DECIMAL(19, 2),
  bet_deposit_amount DECIMAL(19, 2),
  bet_withdrawal_amount DECIMAL(19, 2),
  bet_bonus_amount DECIMAL(19, 2),
  effective_bet_flag TINYINT,
  settle_deposit_amount DECIMAL(19, 2),
  settle_withdrawal_amount DECIMAL(19, 2),
  settle_bonus_amount DECIMAL(19, 2),
  transfer_deposit_amount DECIMAL(19, 2),
  transfer_withdrawal_amount DECIMAL(19, 2),
  transfer_bonus_amount DECIMAL(19, 2),
  third_platform_provider_type STRING,
  bet_time TIMESTAMP(3),
  settle_time TIMESTAMP(3),
  status STRING,
  bet_no STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  game_name_code STRING,
  order_no BIGINT,
  channel_id BIGINT,
  game_record_expand_list STRING,
  valid_bet_amount DECIMAL(19, 2),
  valid_bet_deposit_amount DECIMAL(19, 2),
  valid_bet_withdrawal_amount DECIMAL(19, 2),
  valid_bet_bonus_amount DECIMAL(19, 2),
  third_provider_game_code STRING,
  parent_game_category_id BIGINT,
  third_provider_game_type BIGINT,
  `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,     -- 【重要修改】保留 row_kind 元数据，它表示操作类型
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'gamecenter-test',
    'table-name' = 'game_record_.*',
    'server-id' = '6501-6520', -- 确保配置内有足够的 server-id 分配给并行任务
    -- TODO: 修改原因 - 业务逻辑在新加字段，时区应使用 Asia/Shanghai 保持一致
    -- 'server-time-zone' = 'Asia/Jakarta', -- 原配置
    'server-time-zone' = 'Asia/Bangkok', -- 修改为：统一时区配置为中国时区
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE sink_kafka (
  id BIGINT,
  user_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  bet_amount DECIMAL(19, 2),
  settle_amount DECIMAL(19, 2),
  transfer_amount DECIMAL(19, 2),
  bet_deposit_amount DECIMAL(19, 2),
  bet_withdrawal_amount DECIMAL(19, 2),
  bet_bonus_amount DECIMAL(19, 2),
  effective_bet_flag TINYINT,
  settle_deposit_amount DECIMAL(19, 2),
  settle_withdrawal_amount DECIMAL(19, 2),
  settle_bonus_amount DECIMAL(19, 2),
  transfer_deposit_amount DECIMAL(19, 2),
  transfer_withdrawal_amount DECIMAL(19, 2),
  transfer_bonus_amount DECIMAL(19, 2),
  third_platform_provider_type STRING,
  bet_time TIMESTAMP(3),
  settle_time TIMESTAMP(3),
  status STRING,
  bet_no STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  game_name_code STRING,
  order_no BIGINT,
  channel_id BIGINT,
  game_record_expand_list STRING,
  valid_bet_amount DECIMAL(19, 2),
  valid_bet_deposit_amount DECIMAL(19, 2),
  valid_bet_withdrawal_amount DECIMAL(19, 2),
  valid_bet_bonus_amount DECIMAL(19, 2),
  third_provider_game_code STRING,
  parent_game_category_id BIGINT,
  third_provider_game_type BIGINT
)
WITH
  (
    'connector' = 'kafka',
    'topic' = 'flink_game_order_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    -- 使用 debezium-json 格式可以完美解析 MySQL CDC 的数据流
    'format' = 'debezium-json',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
  );

-- 3. 提交任务
INSERT INTO
  sink_kafka
SELECT
  id,
  user_id,
  game_id,
  game_category_id,
  bet_amount,
  settle_amount,
  transfer_amount,
  bet_deposit_amount,
  bet_withdrawal_amount,
  bet_bonus_amount,
  effective_bet_flag,
  settle_deposit_amount,
  settle_withdrawal_amount,
  settle_bonus_amount,
  transfer_deposit_amount,
  transfer_withdrawal_amount,
  transfer_bonus_amount,
  third_platform_provider_type,
  bet_time,
  settle_time,
  status,
  bet_no,
  create_time,
  update_time,
  game_name_code,
  order_no,
  channel_id,
  game_record_expand_list,
  valid_bet_amount,
  valid_bet_deposit_amount,
  valid_bet_withdrawal_amount,
  valid_bet_bonus_amount,
  third_provider_game_code,
  parent_game_category_id,
  third_provider_game_type
FROM
  source_game_record
WHERE `row_status` <> '-D';

