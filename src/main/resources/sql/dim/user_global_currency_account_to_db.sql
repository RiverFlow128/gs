-- ==========================================
-- DIM层：用户全局币种账户维度表同步
-- 职责：从MySQLCDC读取全局币种账户数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_global_currency_account (业务库)
--       ↓ CDC [本文件]
--   DIM: user_global_currency_account (PolarDB statics库)
--
-- 上游表：MySQL.user_global_currency_account - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_global_currency_account (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 基础运行配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 进阶优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU 核心数调整
SET 'execution.checkpointing.min-pause' = '1min';


CREATE TABLE user_global_currency_account (
    `id` BIGINT,
    `channel_id` BIGINT,
    `user_id` BIGINT,
    `global_currency_num` DECIMAL,
    `create_time` TIMESTAMP,
    `update_time` TIMESTAMP,
    `version` BIGINT,
    `global_currency_status` STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH 
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'user_basic-test',
    'table-name' = 'user_global_currency_account',
    'server-id' = '6661-6680', -- 确保范围内有足够的 server-id 分配给并行任务
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'initial'
  );


CREATE TABLE user_global_currency_account_taget (
    `id` BIGINT,
    `channel_id` BIGINT,
    `user_id` BIGINT,
    `global_currency_num` DECIMAL,
    `create_time` TIMESTAMP,
    `update_time` TIMESTAMP,
    `version` BIGINT,
    `global_currency_status` STRING,
    PRIMARY KEY (id) NOT ENFORCED
)WITH
  (
    'connector' = 'jdbc',
    'table-name' = 'user_global_currency_account',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 1. 开启缓冲写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
  );

-- 3. 提交任务
INSERT INTO
  user_global_currency_account_taget
SELECT
  *
FROM
  user_global_currency_account;