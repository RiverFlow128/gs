-- ==========================================
-- DWS层：充值订单聚合统计
-- 职责：从DWD层读取清洗后的数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_recharge_order (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: today_user_recharge_statistic_dws, today_channel_recharge_statistic (PolarDB)
--
-- 上游表：dwd_recharge_order (Kafka)
-- 上游文件：dwd/recharge_order_dwd.sql
-- 下游表：today_user_recharge_statistic_dws, today_channel_recharge_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- 1. 环境参数调优
SET 'table.local-time-zone' = 'Asia/Singapore';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '20s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.exec.state.ttl' = '36h';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';

-- Checkpoint 安全设置
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '3min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.backend.incremental' = 'true';

-- 2. 定义DWD层源表 (从DWD层Kafka读取清洗后的数据)
CREATE TABLE dwd_recharge_order (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  pay_category_id BIGINT,
  pay_method_id BIGINT,
  pay_channel_id BIGINT,
  pay_provider_id BIGINT,
  id BIGINT,
  order_amount DECIMAL(20, 4),
  actual_handling_fee DECIMAL(20, 4),
  actual_decrease_handling_fee DECIMAL(20, 4),
  actual_gift_amount DECIMAL(20, 4),
  actual_recharge_amount DECIMAL(20, 4),
  pay_status STRING,
  order_status STRING,
  first_recharge INT,
  update_time TIMESTAMP(3),
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_recharge_order',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dws_recharge_order_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

-- 3. 定义目标Sink表 - 用户维度
CREATE TABLE today_user_recharge_statistic_dws (
    statistic_time               DATE,
    user_id                      BIGINT,
    channel_id                   BIGINT,
    abs_id                       BIGINT,
    pay_category_id              BIGINT,
    pay_method_id                BIGINT,
    pay_channel_id               BIGINT,
    pay_provider_id              BIGINT,
    pending_payment_order_count  INT,
    payment_failed_order_count   INT,
    payment_timeout_order_count  INT,
    total_recharge_count         INT,
    recharge_success_order_count INT,
    recharge_failed_order_count  INT,
    recharge_user_count          INT,
    total_recharge_amount        DECIMAL(20, 4),
    success_recharge_amount      DECIMAL(20, 4),
    first_recharge_amount        DECIMAL(20, 4),
    repeat_recharge_amount       DECIMAL(20, 4),
    handling_fee                 DECIMAL(20, 4),
    gift_amount                  DECIMAL(20, 4),
    decrease_handling_fee        DECIMAL(20, 4),
    PRIMARY KEY (statistic_time, user_id, channel_id, pay_category_id, pay_method_id, pay_channel_id, pay_provider_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_recharge_statistic_dws',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s'
);

-- 4. 定义目标Sink表 - 渠道维度
CREATE TABLE today_channel_recharge_statistic (
    channel_id BIGINT,
    statistic_time DATE,
    first_user_count INT,
    first_recharge_amount DECIMAL(20, 4),
    recharge_user_count INT,
    repeat_recharge_amount DECIMAL(20, 4),
    user_count INT,
    pending_payment_order_count INT,
    payment_failed_order_count INT,
    payment_timeout_order_count INT,
    recharge_success_order_count INT,
    handling_fee DECIMAL(20, 4),
    gift_amount DECIMAL(20, 4),
    decrease_handling_fee DECIMAL(20, 4),
    PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_channel_recharge_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '2000',
  'sink.buffer-flush.interval' = '2s'
);

-- 5. 用户维度聚合
INSERT INTO today_user_recharge_statistic_dws
SELECT 
    statistic_time,
    user_id,
    channel_id,
    CAST(-1 AS BIGINT) AS abs_id,
    pay_category_id,
    pay_method_id,
    pay_channel_id,
    pay_provider_id,
    
    CAST(GREATEST(COUNT(IF(pay_status = 'pending_payment', 1, CAST(NULL AS INT))), 0) AS INT) AS pending_payment_order_count,
    CAST(GREATEST(COUNT(IF(pay_status = 'payment_failed', 1, CAST(NULL AS INT))), 0) AS INT) AS payment_failed_order_count,
    CAST(GREATEST(COUNT(IF(pay_status = 'payment_timeout', 1, CAST(NULL AS INT))), 0) AS INT) AS payment_timeout_order_count,
    CAST(GREATEST(COUNT(id), 0) AS INT) AS total_recharge_count,
    CAST(GREATEST(COUNT(IF(order_status = 'complete', 1, CAST(NULL AS INT))), 0) AS INT) AS recharge_success_order_count,
    CAST(GREATEST(COUNT(IF(order_status = 'cancel' OR order_status = 'failed', 1, CAST(NULL AS INT))), 0) AS INT) AS recharge_failed_order_count,
    CAST(1 AS INT) AS recharge_user_count,
    SUM(order_amount) AS total_recharge_amount,
    SUM(IF(order_status = 'complete', actual_recharge_amount, 0)) AS success_recharge_amount,
    SUM(IF(order_status = 'complete' AND first_recharge = 1, order_amount, 0)) AS first_recharge_amount,
    SUM(IF(order_status = 'complete' AND first_recharge = 0, order_amount, 0)) AS repeat_recharge_amount,
    SUM(IF(order_status = 'complete', actual_handling_fee, 0)) AS handling_fee,
    SUM(IF(order_status = 'complete', actual_gift_amount, 0)) AS gift_amount,
    SUM(IF(order_status = 'complete', actual_decrease_handling_fee, 0)) AS decrease_handling_fee
FROM dwd_recharge_order 
GROUP BY 
    statistic_time,
    user_id, 
    channel_id, 
    pay_category_id, 
    pay_method_id, 
    pay_channel_id, 
    pay_provider_id;

-- 6. 渠道维度聚合
INSERT INTO today_channel_recharge_statistic
SELECT
    channel_id,
    statistic_time,
    CAST(COUNT(DISTINCT IF(order_status = 'complete' AND first_recharge = 1, user_id, CAST(NULL AS BIGINT))) AS INT) AS first_user_count,
    SUM(IF(order_status = 'complete' AND first_recharge = 1, order_amount, 0)) AS first_recharge_amount,
    CAST(COUNT(DISTINCT IF(order_status = 'complete', user_id, CAST(NULL AS BIGINT))) AS INT) AS recharge_user_count,
    SUM(IF(order_status = 'complete' AND first_recharge = 0, order_amount, 0)) AS repeat_recharge_amount,
    CAST(COUNT(DISTINCT user_id) AS INT) AS user_count,
    CAST(GREATEST(COUNT(IF(pay_status = 'pending_payment', 1, CAST(NULL AS INT))), 0) AS INT) AS pending_payment_order_count,
    CAST(GREATEST(COUNT(IF(pay_status = 'payment_failed', 1, CAST(NULL AS INT))), 0) AS INT) AS payment_failed_order_count,
    CAST(GREATEST(COUNT(IF(pay_status = 'payment_timeout', 1, CAST(NULL AS INT))), 0) AS INT) AS payment_timeout_order_count,
    CAST(GREATEST(COUNT(IF(order_status = 'complete', 1, CAST(NULL AS INT))), 0) AS INT) AS recharge_success_order_count,
    SUM(IF(order_status = 'complete', actual_handling_fee, 0)) AS handling_fee,
    SUM(IF(order_status = 'complete', actual_gift_amount, 0)) AS gift_amount,
    SUM(IF(order_status = 'complete', actual_decrease_handling_fee, 0)) AS decrease_handling_fee
FROM dwd_recharge_order
GROUP BY
    channel_id,
    statistic_time;
