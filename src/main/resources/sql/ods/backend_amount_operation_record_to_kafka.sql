-- ==========================================
-- ODS层：人工增减记录数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.amount_operation_record (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_amount_operation_record_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_amount_operation_record (Kafka)
--
-- 上游表：MySQL.amount_operation_record (CDC) - 业务库直连
-- 下游表：flink_amount_operation_record_data_sync (Kafka)
-- 下游文件：dwd/amount_operation_record_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 执行优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

-- 确保两个 CP 之间有间隔，减少存储压力
-- 1. 定义源表 (1024切片，只读取增量)
CREATE TABLE source_amount_operation_record (
id BIGINT,
    user_id BIGINT,
    user_phone STRING,
    user_email STRING,
    nick_name STRING,
    `type` STRING,               -- type 是关键字，建议使用反引号
    receive_type STRING,
    amount_type STRING,
    reward_title STRING,
    amount DECIMAL(19, 4),
    flow_amount DECIMAL(19, 4),
    balance DECIMAL(19, 4),
    withdraw_balance DECIMAL(19, 4),
    deposit_balance DECIMAL(19, 4),
    bonus_balance DECIMAL(19, 4),
    bonus_balance_expire_time TIMESTAMP(3),
    flow_balance DECIMAL(19, 4),
    deposit_flow_balance DECIMAL(19, 4),
    bonus_flow_balance DECIMAL(19, 4),
    status STRING,
    remark STRING,
    create_by STRING,
    update_by STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    channel_id BIGINT,
    auditor STRING,
    audit_remark STRING,
    audit_time TIMESTAMP(3),
    import_id BIGINT,
    show_flag TINYINT,
-- 【关键修改】保留 row_kind 元数据
    -- 它会返回诸如 '+I' (Insert), '+U' (Update), '-D' (Delete) 等值
    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'admincenter-test',
    'table-name' = 'amount_operation_record',
    'server-id' = '6561-6580', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE sink_kafka (
id BIGINT,
    user_id BIGINT,
    `type` STRING,               -- 增加：INCREASE, 减少：REDUCE
    amount_type STRING,          -- WITHDRAW, DEPOSIT, BONUS
    amount DECIMAL(19, 4),
    balance DECIMAL(19, 4),
    status STRING,               -- COMPLETE
    create_by STRING,
    update_by STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    channel_id BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_amount_operation_record_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    -- 使用 debezium-json 格式可以方便解析 MySQL CDC 的数据流
    'format' = 'debezium-json', 
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- 3. 提交任务
INSERT INTO sink_kafka -- 假设你已经定义了对应的 JDBC/ClickHouse Sink 表
SELECT 
    id,
    user_id,
    `type`,
    amount_type,
    amount,
    balance,
    status,
    create_by,
    update_by,
    create_time,
    update_time,
    channel_id
FROM source_amount_operation_record
-- TODO: 修改原因 - row_kind元数据中的删除标记是小写'd'而不是'-D'
-- WHERE `row_status` <> '-D';
WHERE `row_status` <> 'd';
