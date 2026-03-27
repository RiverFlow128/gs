-- ==========================================
-- ODS层：提现订单数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.withdraw_order (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_withdraw_order_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_withdraw_order (Kafka)
--
-- 上游表：MySQL.withdraw_order (CDC) - 业务库直连
-- 下游表：flink_withdraw_order_data_sync (Kafka)
-- 下游文件：dwd/withdraw_order_dwd.sql
-- ==========================================

-- 基础运行配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 进阶优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU 核心数调整
SET 'execution.checkpointing.min-pause' = '1min';

-- 确保两个 CP 之间有间歇，减轻存储压力
CREATE TABLE withdraw_order_source (
    id                           BIGINT         comment '主键id' ,
    order_no                     BIGINT         comment '订单号',
    user_id                      BIGINT         comment '用户id',
    channel_id                   BIGINT         comment '渠道id',
    channel                      STRING   comment '渠道',
    withdraw_funds_amount        DECIMAL(20, 4) comment '提现金额',
    balance                      DECIMAL(20, 4) comment '实时余额',
    withdrawal_amount            DECIMAL(20, 4) comment '可提取金额',
    projected_amount             DECIMAL(20, 4) comment '预计到账',
    platform_handling_fee        DECIMAL(20, 4) comment '平台收取的手续费',
    final_handling_fee           DECIMAL(20, 4) comment '最终手续费',
    final_withdraw_funds_amount  DECIMAL(20, 4) comment '最终提现金额',
    actual_amount                DECIMAL(20, 4) comment '实际到账',
    handling_type                STRING         comment '手续费类型',
    handling_mode                STRING         comment '手续费模式',
    handling_detail              STRING         comment '手续费详情',
    gift_mode                    STRING         comment '赠送模式',
    gift_value                   DECIMAL(20, 4) comment '赠送值',
    gift_amount                  DECIMAL(20, 4) comment '赠送金额',
    decrease_handling_mode       STRING         comment '手续费减免模式',
    decrease_handling_value      DECIMAL(20, 4) comment '手续费减免值',
    decrease_handling_amount     DECIMAL(20, 4) comment '手续费减金额',
    apply_ip                     STRING         comment '申请ip',
    user_bank_id                 BIGINT         comment '申请用户银行卡号id',
    apply_name                   STRING         comment '申请人姓名',
    apply_bank_name              STRING         comment '申请银行名称',
    apply_bank_id                BIGINT         comment '申请银行id',
    apply_bank_card              STRING         comment '申请银行卡号',
    apply_email                  STRING         comment '申请人邮箱',
    apply_phone                  STRING         comment '申请人手机号',
    order_status                 STRING         comment '订单状态',
    order_status_event           STRING         comment '订单状态事件',
    remark                       STRING         comment '备注',
    reasons                      STRING         comment '原因',
    front_remarks                STRING         comment '前端备注',
    back_remarks                 STRING         comment '后端备注',
    pay_provider_id              BIGINT         comment '支付商户ID',
    withdrawal_mode_id           BIGINT         comment '提现方式id',
    withdrawal_mode_name         STRING         comment '提现方式名称',
    withdrawal_channel_id        BIGINT         comment '代付渠道Id',
    withdrawal_channel_name      STRING         comment '代付渠道名称',
    withdrawal_channel_bank_code STRING         comment '银行对应的编码',
    pay_provider_type            STRING         comment '支付服务商类型',
    payment_order_no             STRING         comment '当前支付单号',
    third_party_order_no         STRING         comment '第三方平台订单号',
    third_party_handling_fee     DECIMAL(20, 4) comment '第三方收取手续费',
    payments_number              int            comment '代付次数',
    first_withdrawal_flag        TINYINT        comment '是否首次提现',
    create_by                    BIGINT         comment '创建人id',
    create_time                  TIMESTAMP(3)   comment '创建时间',
    update_by                    BIGINT         comment '修改人',
    update_time                  TIMESTAMP(3)   comment '修改时间',
    request_time                 TIMESTAMP(3)   comment '提交第三方时间',
    fallback_time                TIMESTAMP(3)   comment '三方回调时间',
    payout_successful_time       TIMESTAMP(3)   comment '出款成功时间',
    locked_user_id               BIGINT         comment '锁定用户',
    last_audit_user              BIGINT         comment '最后审核人',
    last_audit_time              TIMESTAMP(3)   comment '最后审核时间',
    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,     -- 【关键修复】映射 row_kind 元数据，它代表操作类型 -- c: Create (Insert), u: Update, d: Delete, r: Read (Snapshot)
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
    'table-name' = 'withdraw_order',
    'server-id' = '6541-6560', -- 确保范围内有足够的 server-id 分配给并行任务
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );



-- 2. 定义 Kafka Sink (阿里云 Kafka)
CREATE TABLE withdraw_order_kafka_sink (
    id                           BIGINT         comment '主键id' ,
    order_no                     BIGINT         comment '订单号',
    user_id                      BIGINT         comment '用户id',
    channel_id                   BIGINT         comment '渠道id',
    channel                      STRING   comment '渠道',
    withdraw_funds_amount        DECIMAL(20, 4) comment '提现金额',
    balance                      DECIMAL(20, 4) comment '实时余额',
    withdrawal_amount            DECIMAL(20, 4) comment '可提取金额',
    projected_amount             DECIMAL(20, 4) comment '预计到账',
    platform_handling_fee        DECIMAL(20, 4) comment '平台收取的手续费',
    final_handling_fee           DECIMAL(20, 4) comment '最终手续费',
    final_withdraw_funds_amount  DECIMAL(20, 4) comment '最终提现金额',
    actual_amount                DECIMAL(20, 4) comment '实际到账',
    handling_type                STRING         comment '手续费类型',
    handling_mode                STRING         comment '手续费模式',
    handling_detail              STRING         comment '手续费详情',
    gift_mode                    STRING         comment '赠送模式',
    gift_value                   DECIMAL(20, 4) comment '赠送值',
    gift_amount                  DECIMAL(20, 4) comment '赠送金额',
    decrease_handling_mode       STRING         comment '手续费减免模式',
    decrease_handling_value      DECIMAL(20, 4) comment '手续费减免值',
    decrease_handling_amount     DECIMAL(20, 4) comment '手续费减金额',
    apply_ip                     STRING         comment '申请ip',
    user_bank_id                 BIGINT         comment '申请用户银行卡号id',
    apply_name                   STRING         comment '申请人姓名',
    apply_bank_name              STRING         comment '申请银行名称',
    apply_bank_id                BIGINT         comment '申请银行id',
    apply_bank_card              STRING         comment '申请银行卡号',
    apply_email                  STRING         comment '申请人邮箱',
    apply_phone                  STRING         comment '申请人手机号',
    order_status                 STRING         comment '订单状态',
    order_status_event           STRING         comment '订单状态事件',
    remark                       STRING         comment '备注',
    reasons                      STRING         comment '原因',
    front_remarks                STRING         comment '前端备注',
    back_remarks                 STRING         comment '后端备注',
    pay_provider_id              BIGINT         comment '支付商户ID',
    withdrawal_mode_id           BIGINT         comment '提现方式id',
    withdrawal_mode_name         STRING         comment '提现方式名称',
    withdrawal_channel_id        BIGINT         comment '代付渠道Id',
    withdrawal_channel_name      STRING         comment '代付渠道名称',
    withdrawal_channel_bank_code STRING         comment '银行对应的编码',
    pay_provider_type            STRING         comment '支付服务商类型',
    payment_order_no             STRING         comment '当前支付单号',
    third_party_order_no         STRING         comment '第三方平台订单号',
    third_party_handling_fee     DECIMAL(20, 4) comment '第三方收取手续费',
    payments_number              INT            comment '代付次数',
    first_withdrawal_flag        TINYINT        comment '是否首次提现',
    create_by                    BIGINT         comment '创建人id',
    create_time                  TIMESTAMP(3)   comment '创建时间',
    update_by                    BIGINT         comment '修改人',
    update_time                  TIMESTAMP(3)   comment '修改时间',
    request_time                 TIMESTAMP(3)   comment '提交第三方时间',
    fallback_time                TIMESTAMP(3)   comment '三方回调时间',
    payout_successful_time       TIMESTAMP(3)   comment '出款成功时间',
    locked_user_id               BIGINT         comment '锁定用户',
    last_audit_user              BIGINT         comment '最后审核人',
    last_audit_time              TIMESTAMP(3)   comment '最后审核时间'
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_withdraw_order_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    -- 使用 debezium-json 格式可以完美解析 MySQL CDC 的数据流
    'format' = 'debezium-json', 
    'properties.group.id' = 'flink_withdraw_order_data_sync_group',
    'sink.partitioner' = 'round-robin', -- 或者用 'round-robin' 追求最高吞吐
    'sink.delivery-guarantee' = 'at-least-once'
);

-- 3. 提交任务
INSERT INTO
  withdraw_order_kafka_sink
SELECT
  id,
  order_no,
  user_id,
  channel_id,
  channel,
  withdraw_funds_amount,
  balance,
  withdrawal_amount,
  projected_amount,
  platform_handling_fee,
  final_handling_fee,
  final_withdraw_funds_amount,
  actual_amount,
  handling_type,
  handling_mode,
  handling_detail,
  gift_mode,
  gift_value,
  gift_amount,
  decrease_handling_mode,
  decrease_handling_value,
  decrease_handling_amount,
  apply_ip,
  user_bank_id,
  apply_name,
  apply_bank_name,
  apply_bank_id,
  apply_bank_card,
  apply_email,
  apply_phone,
  order_status,
  order_status_event,
  remark,
  reasons,
  front_remarks,
  back_remarks,
  pay_provider_id,
  withdrawal_mode_id,
  withdrawal_mode_name,
  withdrawal_channel_id,
  withdrawal_channel_name,
  withdrawal_channel_bank_code,
  pay_provider_type,
  payment_order_no,
  third_party_order_no,
  third_party_handling_fee,
  payments_number,
  first_withdrawal_flag,
  create_by,
  create_time,
  update_by,
  update_time,
  request_time,
  fallback_time,
  payout_successful_time,
  locked_user_id,
  last_audit_user,
  last_audit_time
FROM
  withdraw_order_source
WHERE `row_status` <> '-D';


-- ======================================
-- 第二张MySQL源表: third_withdraw_payment_order
-- ======================================

-- 4. 定义 MySQL CDC 源表 (第三方代付订单)
CREATE TABLE third_withdraw_payment_order_source (
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
    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT}',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = 'ordercenter-test',
    'table-name' = 'third_withdraw_payment_order',
    'server-id' = '6561-6580',
    'server-time-zone' = 'Asia/Bangkok',
    'scan.startup.mode' = 'latest-offset'
);


-- 5. 定义 Kafka Sink (第三方代付订单)
CREATE TABLE third_withdraw_payment_order_kafka_sink (
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
    status_reason                STRING         COMMENT '状态原因'
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_third_withdraw_payment_order_data_sync',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format' = 'debezium-json',
    'properties.group.id' = 'flink_third_withdraw_payment_order_data_sync_group',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
);


-- 6. 提交任务 (第三方代付订单)
INSERT INTO third_withdraw_payment_order_kafka_sink
SELECT
    id,
    withdraw_order_no,
    payments_number,
    payment_order_no,
    amount,
    actual_amount,
    apply_name,
    apply_bank_card,
    apply_email,
    apply_phone,
    pay_provider_type,
    third_party_order_no,
    third_party_handling_fee,
    withdrawal_channel_id,
    withdrawal_channel_bank_code,
    status,
    trade_status,
    notify_url,
    request_param,
    response_param,
    create_time,
    update_time,
    pay_provider_id,
    withdrawal_channel_name,
    order_status,
    status_reason
FROM third_withdraw_payment_order_source
WHERE `row_status` <> '-D';