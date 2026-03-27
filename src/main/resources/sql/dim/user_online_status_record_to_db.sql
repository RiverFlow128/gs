-- ==========================================
-- DIM 层：用户在线状态记录同步
-- 职责：从 Kafka 读取用户在线数据，写入 PolarDB 维度表
--
-- 数据血缘：
--   Kafka: user_online (JSON 格式)
--       ↓ Flink SQL 同步 [本文件]
--   DIM: user_online_status_record (PolarDB)
--
-- 上游表：user_online (Kafka)
-- 下游表：user_online_status_record (PolarDB)
-- ==========================================

-- 1. 核心微批优化配置
SET 'table.exec.mini-batch.enabled' = 'true';

-- 允许 3 分钟的延迟，这意味着 Flink 会在内存中攒 3 分钟的数据再往 PolarDB 写，达到降本目的
SET 'table.exec.mini-batch.allow-latency' = '3min';

-- 设置模式为 EXACTLY_ONCE
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 确保 Checkpoint 在任务取消时保留，以便重启恢复
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- 攒批的最大行数
SET 'table.exec.mini-batch.size' = '10000';

-- 开启状态清理，确保内存不会被历史数据撑爆（针对天级报表，设置 36 小时足够）
SET 'table.exec.state.ttl' = '36h';

-- 开启局部聚合优化，解决高基数下的 COUNT DISTINCT 性能问题
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

SET 'execution.checkpointing.interval' = '3min';

-- 2. 定义 Kafka Source 表
-- 核心技巧：将 jsonData 定义为 ROW 类型，方便后续通过 . 访问内部字段
CREATE TABLE kafka_source_user_online (
    `key` STRING,
    `dataSyncType` STRING,
    `jsonData` ROW<
        `user_id` BIGINT,
        `ip` STRING,
        `device_id` STRING,
        `online_time` STRING, -- 先映射为 STRING，后续转换格式
        `create_time` STRING,
        `token` STRING,
        `session_id` STRING
    >
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_online',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink_user_online_group',
    'format' = 'json', -- 注意：这里是标准 JSON，不是 debezium-json
    'scan.startup.mode' = 'earliest-offset', -- 从上次提交的偏移量开始读
    'properties.enable.auto.commit' = 'false' -- 禁用 Kafka 自动提交，交给 Flink 管理
);

-- 3. 定义 MySQL Sink 表 (StarRocks/Doris/MySQL 语法通用)
CREATE TABLE sink_user_online_status (
    `session_id` STRING,
    `user_id` BIGINT,
    `token` STRING,
    `online_time` TIMESTAMP(3),
    `ip` STRING,
    `device_id` STRING,
    `create_time` TIMESTAMP(3),
    PRIMARY KEY (`user_id`, `create_time`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'user_online_status_record',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
);

-- 4. 执行同步任务
INSERT INTO sink_user_online_status
SELECT
    -- 提取嵌套字段并处理默认值
    COALESCE(jsonData.session_id, 'N/A') as session_id,
    jsonData.user_id,
    jsonData.token,
    -- 处理时间格式转换 (ISO 8601 格式 '2025-12-31T15:12:09' 直接支持转换)
    TO_TIMESTAMP(REPLACE(jsonData.online_time, 'T', ' ')),
    jsonData.ip,
    jsonData.device_id,
    TO_TIMESTAMP(REPLACE(jsonData.create_time, 'T', ' '))
FROM kafka_source_user_online;
