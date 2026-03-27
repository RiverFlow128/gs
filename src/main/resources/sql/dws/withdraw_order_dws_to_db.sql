-- ==========================================
-- DWS层：提现订单聚合统计
-- 职责：从DWD层读取清洗后的数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_withdraw_order (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: today_user_withdraw_statistic_dws, today_channel_withdraw_statistic,
--        today_user_withdraw_statistic_finance_increment (PolarDB)
--
-- 上游表：dwd_withdraw_order (Kafka)
-- 上游文件：dwd/withdraw_order_dwd.sql
-- 下游表：today_user_withdraw_statistic_dws, today_channel_withdraw_statistic,
--         today_user_withdraw_statistic_finance_increment (PolarDB)
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
CREATE TABLE dwd_withdraw_order (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  apply_bank_id BIGINT,
  withdrawal_mode_id BIGINT,
  withdrawal_channel_id BIGINT,
  pay_provider_id BIGINT,
  id BIGINT,
  order_no BIGINT,
  withdraw_funds_amount DECIMAL(20, 4),
  final_handling_fee DECIMAL(20, 4),
  gift_amount DECIMAL(20, 4),
  decrease_handling_amount DECIMAL(20, 4),
  order_status STRING,
  first_withdrawal_flag TINYINT,
  update_time TIMESTAMP(3),
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_withdraw_order',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dws_withdraw_order_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);


-- 2.1 定义第三方代付订单Kafka源表 (用于finance_increment聚合)
CREATE TABLE third_withdraw_payment_order_kafka (
    id                           BIGINT         COMMENT '主键id',
    withdraw_order_no            BIGINT         COMMENT '提现订单号',
    payments_number              INT            COMMENT '第几次支付',
    payment_order_no             STRING         COMMENT '支付单号',
    amount                       STRING         COMMENT '提现金额',
    actual_amount                STRING         COMMENT '实际到账',
    apply_name                   STRING         COMMENT '申请人姓名',
    apply_bank_card              STRING         COMMENT '申请银行卡号',
    apply_email                  STRING         COMMENT '申请人邮箱',
    apply_phone                  STRING         COMMENT '申请人手机号',
    pay_provider_type            STRING         COMMENT '支付服务商类型',
    third_party_order_no         STRING         COMMENT '第三方平台订单号',
    third_party_handling_fee     STRING         COMMENT '第三方手续费',
    withdrawal_channel_id        BIGINT         COMMENT '代付渠道Id',
    withdrawal_channel_bank_code STRING         COMMENT '银行对应的编码',
    status                       STRING         COMMENT '状态',
    trade_status                 STRING         COMMENT '交易状态',
    notify_url                   STRING         COMMENT '回调地址',
    request_param                STRING         COMMENT '请求参数',
    response_param               STRING         COMMENT '响应参数',
    create_time                  TIMESTAMP(3)   COMMENT '创建时间',
    update_time                  TIMESTAMP(3)   COMMENT '修改时间',
    pay_provider_id              BIGINT         COMMENT '代付商户Id',
    withdrawal_channel_name      STRING         COMMENT '代付渠道名称',
    order_status                 STRING         COMMENT '订单状态',
    status_reason                STRING         COMMENT '状态原因',
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_third_withdraw_payment_order_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'dws_third_withdraw_payment_order_group',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset',
    'properties.enable.auto.commit' = 'false'
);

-- 3. 定义目标Sink表 - 用户维度
CREATE TABLE today_user_withdraw_statistic_dws (
    statistic_time DATE,
    user_id BIGINT,
    channel_id BIGINT,
    pay_category_id BIGINT,
    pay_method_id BIGINT,
    pay_channel_id BIGINT,
    pay_provider_id BIGINT,
    completed_withdraw_amount DECIMAL(20, 4),
    rejected_withdraw_amount DECIMAL(20, 4),
    success_withdraw_amount DECIMAL(20, 4),
    first_withdraw_amount DECIMAL(20, 4),
    handling_fee DECIMAL(20, 4),
    gift_amount DECIMAL(20, 4),
    decrease_handling_fee DECIMAL(20, 4),
    withdraw_order_count INT,
    withdraw_success_order_count INT,
    rejected_order_count INT,
    completed_order_count INT,
    PRIMARY KEY (statistic_time, user_id, channel_id, pay_category_id, pay_method_id, pay_channel_id, pay_provider_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_withdraw_statistic_dws',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s'
);

-- 4. 定义目标Sink表 - 渠道维度
CREATE TABLE today_channel_withdraw_statistic (
    user_count INT,
    statistic_time DATE,
    channel_id BIGINT,
    withdraw_amount DECIMAL(20, 4),
    handling_fee DECIMAL(20, 4),
    gift_amount DECIMAL(20, 4),
    decrease_handling_fee DECIMAL(20, 4),
    rejected_order_count INT,
    completed_order_count INT,
    first_user_count INT,
    PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_channel_withdraw_statistic',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '2000',
  'sink.buffer-flush.interval' = '2s'
);


-- 4.1 定义目标Sink表 - 财务增量维度 (需要LEFT JOIN third_withdraw_payment_order)
CREATE TABLE today_user_withdraw_statistic_finance_increment (
    statistic_time               DATE           COMMENT '统计日期',
    user_id                      BIGINT         COMMENT '用户ID',
    channel_id                   BIGINT         COMMENT '子渠道ID',
    pay_category_id              BIGINT         COMMENT '支付类型ID',
    pay_method_id                BIGINT         COMMENT '支付方式ID',
    pay_channel_id               BIGINT         COMMENT '支付渠道ID',
    pay_provider_id              BIGINT         COMMENT '订单支付供应商ID',
    withdraw_amount              DECIMAL(20, 4) COMMENT '提现总额',
    success_withdraw_amount      DECIMAL(20, 4) COMMENT '提现成功金额',
    user_count                   BIGINT         COMMENT '提现人数',
    withdraw_order_count         BIGINT         COMMENT '提现总订单数',
    withdraw_success_order_count BIGINT         COMMENT '成功提现订单数',
    create_time                  TIMESTAMP(3)   COMMENT '创建时间',
    update_time                  TIMESTAMP(3)   COMMENT '修改时间',
    PRIMARY KEY (statistic_time, user_id, channel_id, pay_category_id, pay_method_id, pay_channel_id, pay_provider_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
  'table-name' = 'today_user_withdraw_statistic_finance_increment',
  'username' = 'super',
  'password' = '${MYSQL_PASSWORD}',
  'sink.buffer-flush.max-rows' = '2000',
  'sink.buffer-flush.interval' = '2s'
);

-- 5. 用户维度聚合
INSERT INTO today_user_withdraw_statistic_dws
SELECT 
    statistic_time,
    user_id,
    channel_id,
    apply_bank_id AS pay_category_id,
    withdrawal_mode_id AS pay_method_id,
    withdrawal_channel_id AS pay_channel_id,
    pay_provider_id,
    SUM(IF(order_status = 'COMPLETED', withdraw_funds_amount, 0)) AS completed_withdraw_amount,
    SUM(IF(order_status = 'REJECTED', withdraw_funds_amount, 0)) AS rejected_withdraw_amount,
    SUM(IF(order_status = 'COMPLETED', withdraw_funds_amount, 0)) AS success_withdraw_amount,
    SUM(IF((order_status = 'COMPLETED') AND (first_withdrawal_flag = 1), withdraw_funds_amount, 0)) AS first_withdraw_amount,
    SUM(IF(order_status = 'COMPLETED', final_handling_fee, 0)) AS handling_fee,
    SUM(IF(order_status = 'COMPLETED', gift_amount, 0)) AS gift_amount,
    SUM(IF(order_status = 'COMPLETED', decrease_handling_amount, 0)) AS decrease_handling_fee,
    COUNT(id) AS withdraw_order_count,
    COUNT(DISTINCT IF(order_status = 'COMPLETED', order_no, CAST(NULL AS BIGINT))) AS withdraw_success_order_count,
    COUNT(DISTINCT IF(order_status = 'REJECTED', order_no, CAST(NULL AS BIGINT))) AS rejected_order_count,
    COUNT(DISTINCT IF(order_status = 'COMPLETED', order_no, CAST(NULL AS BIGINT))) AS completed_order_count
FROM dwd_withdraw_order
GROUP BY 
    statistic_time,
    user_id,
    channel_id,
    apply_bank_id,
    withdrawal_mode_id,
    withdrawal_channel_id,
    pay_provider_id;

-- 6. 渠道维度聚合
INSERT INTO today_channel_withdraw_statistic
SELECT
    COUNT(DISTINCT user_id) AS user_count,
    statistic_time,
    channel_id,
    SUM(withdraw_funds_amount) AS withdraw_amount,
    SUM(IF(order_status = 'COMPLETED', final_handling_fee, 0)) AS handling_fee,
    SUM(IF(order_status = 'COMPLETED', gift_amount, 0)) AS gift_amount,
    SUM(IF(order_status = 'COMPLETED', decrease_handling_amount, 0)) AS decrease_handling_fee,
    COUNT(DISTINCT IF(order_status = 'REJECTED', order_no, CAST(NULL AS BIGINT))) AS rejected_order_count,
    COUNT(DISTINCT IF(order_status = 'COMPLETED', order_no, CAST(NULL AS BIGINT))) AS completed_order_count,
    COUNT(DISTINCT IF(order_status = 'COMPLETED', user_id, CAST(NULL AS BIGINT))) AS first_user_count
FROM dwd_withdraw_order
GROUP BY
    statistic_time,
    channel_id;


-- 7. 财务增量维度聚合 (需要LEFT JOIN third_withdraw_payment_order_kafka)
INSERT INTO today_user_withdraw_statistic_finance_increment
SELECT 
    CAST(t2.update_time AS DATE) AS statistic_time,
    t1.user_id,
    t1.channel_id,
    t1.apply_bank_id AS pay_category_id,
    t1.withdrawal_mode_id AS pay_method_id,
    COALESCE(t2.withdrawal_channel_id, 0) AS pay_channel_id,
    COALESCE(t2.pay_provider_id, 0) AS pay_provider_id,
    SUM(CAST(t2.amount AS DECIMAL(20, 4))) AS withdraw_amount,
    SUM(IF(t1.order_status = 'COMPLETED', t1.withdraw_funds_amount, 0)) AS success_withdraw_amount,
    COUNT(DISTINCT t1.user_id) AS user_count,
    COUNT(DISTINCT t2.payment_order_no) AS withdraw_order_count,
    COUNT(IF(t1.order_status = 'COMPLETED', 1, CAST(NULL AS BIGINT))) AS withdraw_success_order_count,
    MAX(t1.update_time) AS create_time,
    MAX(t1.update_time) AS update_time
FROM dwd_withdraw_order AS t1
LEFT JOIN third_withdraw_payment_order_kafka t2 
    ON t2.withdraw_order_no = t1.order_no
    AND CAST(t2.update_time AS DATE) = t1.statistic_time
WHERE t2.order_status IN ('COMPLETED', 'WITHDRAW_FAIL')
GROUP BY 
    CAST(t2.update_time AS DATE),
    t1.user_id,
    t1.channel_id,
    t1.apply_bank_id,
    t1.withdrawal_mode_id,
    COALESCE(t2.withdrawal_channel_id, 0),
    COALESCE(t2.pay_provider_id, 0);
