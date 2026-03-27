-- ==========================================
-- ODS层：充值订单数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.recharge_order (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_recharge_order_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_recharge_order (Kafka)
--
-- 上游表：MySQL.recharge_order (CDC) - 业务库直连
-- 下游表：flink_recharge_order_data_sync (Kafka)
-- 下游文件：dwd/recharge_order_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 执行优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

-- 确保两个 CP 之间有间隔，减少存储压力
CREATE TABLE recharge_order_source (
    id                           BIGINT,
    order_no                     BIGINT,
    user_id                      BIGINT,
    currency_code                STRING,
    rate_scale                   DECIMAL(20, 4),
    order_amount                 DECIMAL(20, 4),
    pay_amount                   DECIMAL(20, 4),
    decrease_handling_fee        DECIMAL(20, 4),
    handling_fee                 DECIMAL(20, 4),
    gift_amount                  DECIMAL(20, 4),
    recharge_amount              DECIMAL(20, 4),
    actual_handling_fee          DECIMAL(20, 4),
    actual_decrease_handling_fee DECIMAL(20, 4),
    actual_gift_amount           DECIMAL(20, 4),
    actual_recharge_amount       DECIMAL(20, 4),
    pay_status                   String,
    order_status                 String,
    first_recharge               TINYINT,
    pay_method_id                BIGINT,
    pay_category_id              BIGINT,
    pay_channel_id               BIGINT,
    pay_provider_id              BIGINT,
    recharge_success_time        TIMESTAMP(3),
    create_time                  TIMESTAMP(3),
    update_time                  TIMESTAMP(3),
    update_by                    STRING,
    channel_id                   BIGINT,
    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,     -- 【关键修改】保留 row_kind 元数据，表示操作类型
    -- c: Create (Insert), u: Update, d: Delete, r: Read (Snapshot)
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'ordercenter-test',
    'table-name' = 'recharge_order',
    'server-id' = '6521-6540', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Bangkok',
    -- mysql-cdc中应设置为latest-offset表示从最新位置开始读
    'scan.startup.mode' = 'latest-offset'
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE recharge_order_kafka_sink (
    id                           BIGINT,
    order_no                     BIGINT,
    user_id                      BIGINT,
    currency_code                STRING,
    rate_scale                   DECIMAL(20, 4),
    order_amount                 DECIMAL(20, 4),
    pay_amount                   DECIMAL(20, 4),
    decrease_handling_fee        DECIMAL(20, 4),
    handling_fee                 DECIMAL(20, 4),
    gift_amount                  DECIMAL(20, 4),
    recharge_amount              DECIMAL(20, 4),
    actual_handling_fee          DECIMAL(20, 4),
    actual_decrease_handling_fee DECIMAL(20, 4),
    actual_gift_amount           DECIMAL(20, 4),
    actual_recharge_amount       DECIMAL(20, 4),
    pay_status                   String,
    order_status                 String,
    first_recharge               TINYINT,
    pay_method_id                BIGINT,
    pay_category_id              BIGINT,
    pay_channel_id               BIGINT,
    pay_provider_id              BIGINT,
    recharge_success_time        TIMESTAMP(3),
    create_time                  TIMESTAMP(3),
    update_time                  TIMESTAMP(3),
    update_by                    STRING,
    channel_id                   BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_recharge_order_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    -- 使用 debezium-json 格式可以方便解析 MySQL CDC 的数据流
    'format' = 'debezium-json',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- 3. 提交任务
INSERT INTO
  recharge_order_kafka_sink
SELECT
  id,
  order_no,
  user_id,
  currency_code,
  rate_scale,
  order_amount,
  pay_amount,
  decrease_handling_fee,
  handling_fee,
  gift_amount,
  recharge_amount,
  actual_handling_fee,
  actual_decrease_handling_fee,
  actual_gift_amount,
  actual_recharge_amount,
  pay_status,
  order_status,
  first_recharge,
  pay_method_id,
  pay_category_id,
  pay_channel_id,
  pay_provider_id,
  recharge_success_time,
  create_time,
  update_time,
  update_by,
  channel_id
FROM
  recharge_order_source
-- TODO: 修改原因 - row_kind元数据中的删除标记是'd'(小写)，不是'-D'。'c':Insert, 'u':Update, 'd':Delete
WHERE `row_status` <> 'd';
