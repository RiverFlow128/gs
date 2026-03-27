-- ==========================================
-- DIM层：用户关系维度表同步
-- 职责：从MySQLCDC读取用户关系数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_relations (业务库)
--       ↓ CDC [本文件]
--   DIM: user_relations (PolarDB statics库)
--
-- 上游表：MySQL.user_relations - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_relations (PolarDB)
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

--1.数据库表信息
CREATE TABLE user_relations_source (
    id BIGINT,
    distance INT,
    ancestor_user_id BIGINT,
    ancestor_channel_id BIGINT,
    descendant_user_id BIGINT,
    descendant_channel_id BIGINT,
    invite_Code STRING,
    recharge_rebate_amount DECIMAL(20,4),
    bet_rebate_amount DECIMAL(20,4),
    rebate_amount DECIMAL(20,4),
    effective_time TIMESTAMP(3),
    betting_rebate_count INT,
    recharge_rebate_count INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(6),

    PRIMARY KEY (id, create_time) NOT ENFORCED
) WITH 
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'user_basic-test',
    'table-name' = 'user_relations',
    'server-id' = '6621-6640', -- 确保范围内有足够的 server-id 分配给并行任务
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'initial'
  );



-- 3. 定义 PolarDB 目标表
CREATE TABLE user_relations_taget (
    id BIGINT,
    distance INT,
    ancestor_user_id BIGINT,
    ancestor_channel_id BIGINT,
    descendant_user_id BIGINT,
    descendant_channel_id BIGINT,
    invite_Code STRING,
    recharge_rebate_amount DECIMAL(20,4),
    bet_rebate_amount DECIMAL(20,4),
    rebate_amount DECIMAL(20,4),
    effective_time TIMESTAMP(3),
    betting_rebate_count INT,
    recharge_rebate_count INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(6),

    PRIMARY KEY (id, create_time) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'user_relations',
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
  user_relations_taget
SELECT
  *
FROM
  user_relations_source;