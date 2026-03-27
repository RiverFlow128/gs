-- ==========================================
-- DWS层：游戏记录聚合统计
-- 职责：从ODS层Kafka读取原始数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   ODS: flink_game_order_data_sync (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: game_record, today_user_game_statistics_dws, today_user_hour_game_ug_statistics_data,
--        today_channel_game_category_statistic, today_channel_game_provider_statistic,
--        today_channel_game_record_statistic (PolarDB)
--
-- 上游表：flink_game_order_data_sync (Kafka)
-- 上游文件：ods/game_record_all_to_kafka.sql
-- 下游表：game_record, today_user_game_statistics_dws 等 (PolarDB)
-- 下游文件：ads/today_user_game_statistic.sql (从DWS表CDC读取)
-- ==========================================

-- 1. 核心微批优化配置
SET 'table.exec.mini-batch.enabled' = 'true';

-- TODO: 注释有误，mini-batch 控制的是聚合计算攒批，不是写库频率
-- 允许 3 分钟的延迟，这意味着 Flink 会在内存中攒 3 分钟的数据再往 PolarDB 写，达到降本目的
SET 'table.exec.mini-batch.allow-latency' = '3min';
-- 1秒快速处理，避免算子内积压
--  SET 'table.exec.mini-batch.allow-latency' = '1s';

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

SET 'table.local-time-zone' = 'Asia/Shanghai';  -- 统一使用中国时区

-- 失败率重启策略
SET 'restart-strategy' = 'failure-rate';
SET 'restart-strategy.failure-rate.max-failures-per-interval' = '5';   -- 5 分钟内最多 5 次失败
SET 'restart-strategy.failure-rate.failure-rate-interval' = '5min';    -- 时间窗口 5 分钟
SET 'restart-strategy.failure-rate.delay' = '30s';                      -- 每次重试间隔 30 秒

-- 关键：防止统计日期漂移
-- 2. 定义 Kafka 源表 (不需要定义 Watermark)
CREATE TABLE kafka_game_record (
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
PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'kafka',
    'topic' = 'flink_game_order_data_sync',
    'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
    'properties.group.id' = 'flink_game_order_data_sync_group',
    'format' = 'debezium-json', -- 自动识别 Insert/Update/Delete
    'scan.startup.mode' = 'earliest-offset', -- 从上次提交的偏移量开始读
    'properties.enable.auto.commit' = 'false' -- 禁用 Kafka 自动提交，交给 Flink 管理
  );

-- 2. 定义目标表 (测试环境数据不对问题)
CREATE TABLE polardb_game_record (
  id bigint comment '主键id',
  user_id bigint comment '用户ID',
  game_id bigint comment '游戏的id',
  game_category_id bigint comment '游戏类别ID',
  bet_amount decimal(20, 4) comment '投注金额',
  settle_amount decimal(20, 4) comment '结算金额',
  transfer_amount decimal(20, 4) comment '（游戏输赢）结算金额-投注金额 = 转账金额',
  bet_deposit_amount decimal(20, 4) comment '下注锁定金额',
  bet_withdrawal_amount decimal(20, 4) comment '下注提现金额',
  bet_bonus_amount decimal(20, 4) comment '下注奖金金额',
  effective_bet_flag TINYINT comment '是否为有效投注',
  settle_deposit_amount decimal(20, 4) comment '结算锁定金额',
  settle_withdrawal_amount decimal(20, 4) comment '结算提现金额',
  settle_bonus_amount decimal(20, 4) comment '结算奖金金额',
  transfer_deposit_amount decimal(20, 4) comment '(锁定游戏输赢)转账锁定金额：结算金额-投注金额 = 转账金额',
  transfer_withdrawal_amount decimal(20, 4) comment '(提现游戏输赢)转账提现金额：结算金额-投注金额 = 转账金额',
  transfer_bonus_amount decimal(20, 4) comment '(奖金游戏输赢)转账奖金金额：结算金额-投注金额 = 转账金额',
  third_platform_provider_type varchar(255) comment '第三方游戏平台ID',
  bet_time TIMESTAMP(3) comment '投注时间',
  settle_time TIMESTAMP(3) comment '投注结算时间: 投注回调我们自己平台的处理时间',
  status varchar(255) comment '结算状态',
  bet_no varchar(255) comment '下注单号',
  create_time TIMESTAMP(3) comment '创建时间',
  update_time TIMESTAMP(3) comment '更新时间',
  game_name_code varchar(255) comment '游戏名称code',
  order_no bigint comment '本平台单号',
  channel_id bigint comment 'channel_id',
  valid_bet_amount decimal(20, 4) comment '有效投注金额',
  valid_bet_deposit_amount decimal(20, 4) comment '有效投注金额-锁定金额',
  valid_bet_withdrawal_amount decimal(20, 4) comment '有效投注金额-可提现金额',
  valid_bet_bonus_amount decimal(20, 4) comment '有效投注金额-奖金金额',
  third_provider_game_code varchar(255) comment '第三方游戏id',
  parent_game_category_id bigint comment '游戏父类别id',
  third_provider_game_type bigint comment '厂商游戏类型ID',
  primary key (order_no, create_time) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'game_record',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '6',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 3. 定义 PolarDB 目标表
CREATE TABLE polar_game_statistics_dws (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  game_id BIGINT,
  game_category_id BIGINT,
  third_platform_provider_type STRING,
  bet_count BIGINT,
  effective_bet_count BIGINT,
  settle_count BIGINT,
  settle_bet_amount DECIMAL(20, 4),
  bet_amount DECIMAL(20, 4),
  settle_amount DECIMAL(20, 4),
  win_lose_amount DECIMAL(20, 4),
  effective_bet_amount DECIMAL(20, 4),
  bet_deposit_amount DECIMAL(20, 4),
  bet_withdrawal_amount DECIMAL(20, 4),
  bet_bonus_amount DECIMAL(20, 4),
  settle_deposit_amount DECIMAL(20, 4),
  settle_withdrawal_amount DECIMAL(20, 4),
  settle_bonus_amount DECIMAL(20, 4),
  win_lose_deposit_amount DECIMAL(20, 4),
  win_lose_withdrawal_amount DECIMAL(20, 4),
  win_lose_bonus_amount DECIMAL(20, 4),
  effective_bet_deposit_amount DECIMAL(20, 4),
  effective_bet_withdrawal_amount DECIMAL(20, 4),
  effective_bet_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (
    statistic_time,
    user_id,
    game_id,
    game_category_id,
    channel_id,
    third_platform_provider_type
  ) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_game_statistics_dws',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '6',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 3. 定义 PolarDB 目标表-带时区的用户维度UG游戏汇总
CREATE TABLE polar_user_hour_game_ug_statistics (
  user_id bigint comment '用户ID',
  game_id bigint comment '游戏分类ID',
  channel_id bigint comment '渠道ID',
  third_platform_provider_type STRING comment '第三方游戏平台ID',
  statistic_time date comment '统计日期',
  statistic_hour bigint comment '统计时段(0-23)',
  bet_count bigint comment '投注数量',
  effective_bet_count bigint comment '有效投注数量',
  settle_count bigint comment '已结算注单数量',
  profit_and_loss_amount decimal(20, 4) comment '损益金额（公式：投注金额-结算金额）',
  bet_amount decimal(20, 4) comment '投注总金额',
  settle_bet_amount decimal(20, 4) comment '投注金额（已结算）',
  settle_amount decimal(20, 4) comment '结算金额',
  win_lose_amount decimal(20, 4) comment '输赢金额（转账金额，公式：结算金额-投注金额）',
  effective_bet_amount decimal(20, 4) comment '有效投注金额',
  bet_deposit_amount decimal(20, 4) comment '投注锁定金额',
  bet_withdrawal_amount decimal(20, 4) comment '投注可提现金额',
  bet_bonus_amount decimal(20, 4) comment '投注奖金金额',
  settle_deposit_amount decimal(20, 4) comment '结算锁定金额',
  settle_withdrawal_amount decimal(20, 4) comment '结算提现金额',
  settle_bonus_amount decimal(20, 4) comment '结算奖金金额',
  win_lose_deposit_amount decimal(20, 4) comment '输赢锁定金额',
  win_lose_withdrawal_amount decimal(20, 4) comment '输赢提现金额',
  win_lose_bonus_amount decimal(20, 4) comment '输赢奖金金额',
  effective_bet_deposit_amount decimal(20, 4) comment '有效投注锁定金额',
  effective_bet_withdrawal_amount decimal(20, 4) comment '有效投注提现金额',
  effective_bet_bonus_amount decimal(20, 4) comment '有效投注奖金金额',
  PRIMARY KEY (
    statistic_time,
    statistic_hour,
    user_id,
    channel_id,
    game_id,
    third_platform_provider_type
  ) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_hour_game_ug_statistics_data',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '10',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 表 1：渠道 + 游戏分类统计
CREATE TABLE polar_channel_category_statistic (
  game_category_id BIGINT,
  channel_id BIGINT,
  statistic_time DATE,
  user_count BIGINT,
  bet_count BIGINT,
  effective_bet_count BIGINT,
  settle_count BIGINT,
  settle_bet_amount DECIMAL(20, 4),
  bet_amount DECIMAL(20, 4),
  settle_amount DECIMAL(20, 4),
  win_lose_amount DECIMAL(20, 4),
  effective_bet_amount DECIMAL(20, 4),
  bet_deposit_amount DECIMAL(20, 4),
  bet_withdrawal_amount DECIMAL(20, 4),
  bet_bonus_amount DECIMAL(20, 4),
  settle_deposit_amount DECIMAL(20, 4),
  settle_withdrawal_amount DECIMAL(20, 4),
  settle_bonus_amount DECIMAL(20, 4),
  win_lose_deposit_amount DECIMAL(20, 4),
  win_lose_withdrawal_amount DECIMAL(20, 4),
  win_lose_bonus_amount DECIMAL(20, 4),
  effective_bet_deposit_amount DECIMAL(20, 4),
  effective_bet_withdrawal_amount DECIMAL(20, 4),
  effective_bet_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (statistic_time, channel_id, game_category_id) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_game_category_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '5',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 表 2：渠道 + 游戏厂商统计
CREATE TABLE polar_channel_provider_statistic (
  channel_id BIGINT,
  statistic_time DATE,
  third_platform_provider_type STRING,
  user_count BIGINT,
  bet_count BIGINT,
  settle_count BIGINT,
  effective_bet_count BIGINT,
  settle_bet_amount DECIMAL(20, 4),
  bet_amount DECIMAL(20, 4),
  settle_amount DECIMAL(20, 4),
  win_lose_amount DECIMAL(20, 4),
  effective_bet_amount DECIMAL(20, 4),
  bet_deposit_amount DECIMAL(20, 4),
  bet_withdrawal_amount DECIMAL(20, 4),
  bet_bonus_amount DECIMAL(20, 4),
  settle_deposit_amount DECIMAL(20, 4),
  settle_withdrawal_amount DECIMAL(20, 4),
  settle_bonus_amount DECIMAL(20, 4),
  win_lose_deposit_amount DECIMAL(20, 4),
  win_lose_withdrawal_amount DECIMAL(20, 4),
  win_lose_bonus_amount DECIMAL(20, 4),
  effective_bet_deposit_amount DECIMAL(20, 4),
  effective_bet_withdrawal_amount DECIMAL(20, 4),
  effective_bet_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (
    statistic_time,
    channel_id,
    third_platform_provider_type
  ) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_game_provider_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '5',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 表 3：渠道纯维度统计
CREATE TABLE polar_channel_record_statistic (
  channel_id BIGINT,
  statistic_time DATE,
  settle_count BIGINT,
  user_count BIGINT,
  bet_count BIGINT,
  effective_bet_count BIGINT,
  settle_bet_amount DECIMAL(20, 4),
  bet_amount DECIMAL(20, 4),
  settle_amount DECIMAL(20, 4),
  win_lose_amount DECIMAL(20, 4),
  effective_bet_amount DECIMAL(20, 4),
  bet_deposit_amount DECIMAL(20, 4),
  bet_withdrawal_amount DECIMAL(20, 4),
  bet_bonus_amount DECIMAL(20, 4),
  settle_deposit_amount DECIMAL(20, 4),
  settle_withdrawal_amount DECIMAL(20, 4),
  settle_bonus_amount DECIMAL(20, 4),
  win_lose_deposit_amount DECIMAL(20, 4),
  win_lose_withdrawal_amount DECIMAL(20, 4),
  win_lose_bonus_amount DECIMAL(20, 4),
  effective_bet_deposit_amount DECIMAL(20, 4),
  effective_bet_withdrawal_amount DECIMAL(20, 4),
  effective_bet_bonus_amount DECIMAL(20, 4),
  PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_game_record_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    -- 2. 写入并发控制
    'sink.parallelism' = '5',
    --TODO 批量写入，提高吞吐
    'sink.buffer-flush.max-rows' = '5000',      -- 批量大小
    'sink.buffer-flush.interval' = '5s',        -- 批量超时
    'sink.max-retries' = '3'                    -- 写入失败重试次数（关键！）
  );

-- 后面不需要可以删除
INSERT INTO
  polardb_game_record
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
  valid_bet_amount,
  valid_bet_deposit_amount,
  valid_bet_withdrawal_amount,
  valid_bet_bonus_amount,
  third_provider_game_code,
  parent_game_category_id,
  third_provider_game_type
FROM
  kafka_game_record
where order_no is not null;

-- 4. 增量汇总逻辑 - 不带时区的用户维度宽表
INSERT INTO
  polar_game_statistics_dws
SELECT
  CAST(update_time AS DATE) AS statistic_time,
  user_id,
  channel_id,
  game_id,
  game_category_id,
  third_platform_provider_type,
  COUNT(order_no) AS bet_count,
  SUM(IF(status = 'settle', CAST(effective_bet_flag AS BIGINT), 0)) AS effective_bet_count,
  SUM(IF(status = 'settle', 1, 0)) AS settle_count,
  SUM(IF(status = 'settle', bet_amount, 0)) AS settle_bet_amount,
  SUM(bet_amount) AS bet_amount,
  SUM(IF(status = 'settle', settle_amount, 0)) AS settle_amount,
  SUM(IF(status = 'settle', transfer_amount, 0)) AS win_lose_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_amount, 0)) AS effective_bet_amount,
  SUM(bet_deposit_amount) AS bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS bet_bonus_amount,
  SUM(settle_deposit_amount) AS settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS settle_bonus_amount,
  SUM(transfer_deposit_amount) AS win_lose_deposit_amount,
  SUM(transfer_withdrawal_amount) AS win_lose_withdrawal_amount,
  SUM(transfer_bonus_amount) AS win_lose_bonus_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_deposit_amount, 0)) AS effective_bet_deposit_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_withdrawal_amount, 0)) AS effective_bet_withdrawal_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_bonus_amount, 0)) AS effective_bet_bonus_amount
FROM
  kafka_game_record
where
  status = 'settle'
GROUP BY
  CAST(update_time AS DATE),
  user_id,
  channel_id,
  game_id,
  game_category_id,
  third_platform_provider_type;

-- 用户维度带时区的游戏UG表
INSERT INTO
  polar_user_hour_game_ug_statistics
SELECT user_id                                                                          AS user_id,
  game_id                                                                               as game_id,
  channel_id                                                                            as channel_id,
  third_platform_provider_type                                                          as third_platform_provider_type,
  CAST(update_time AS DATE)                                                             AS statistic_time,
  HOUR(update_time)                                                                     AS statistic_hour,
  COUNT(order_no)                                                                       AS bet_count,
  SUM(IF(status = 'settle', effective_bet_flag, 0))                                     AS effective_bet_count,
  SUM(IF(status = 'settle', 1, 0))                                                      AS settle_count,
  sum(if(status = 'settle', -transfer_amount, 0))                                       AS profit_and_loss_amount,
  SUM(bet_amount)                                                                       AS bet_amount,
  SUM(IF(status = 'settle', bet_amount, 0))                                             AS settle_bet_amount,
  SUM(IF(status = 'settle', settle_amount, 0))                                          AS settle_amount,
  SUM(IF(status = 'settle', transfer_amount, 0))                                        AS win_lose_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_amount, 0))            AS effective_bet_amount,
  SUM(bet_deposit_amount)                                                               AS bet_deposit_amount,
  SUM(bet_withdrawal_amount)                                                            AS bet_withdrawal_amount,
  SUM(bet_bonus_amount)                                                                 AS bet_bonus_amount,
  SUM(settle_deposit_amount)                                                            AS settle_deposit_amount,
  SUM(settle_withdrawal_amount)                                                         AS settle_withdrawal_amount,
  SUM(settle_bonus_amount)                                                              AS settle_bonus_amount,
  SUM(transfer_deposit_amount)                                                          AS win_lose_deposit_amount,
  SUM(transfer_withdrawal_amount)                                                       AS win_lose_withdrawal_amount,
  SUM(transfer_bonus_amount)                                                            AS win_lose_bonus_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_deposit_amount, 0))    AS effective_bet_deposit_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_withdrawal_amount, 0)) AS effective_bet_withdrawal_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_bonus_amount, 0))      AS effective_bet_bonus_amount
FROM kafka_game_record
where status = 'settle'
GROUP BY CAST(update_time AS DATE),
    HOUR(update_time),
    user_id,
    channel_id,
    game_id,
    game_category_id,
    third_platform_provider_type;

-- ==========================================
-- 1. 渠道 + 游戏分类聚合
-- ==========================================
INSERT INTO
  polar_channel_category_statistic
SELECT
  game_category_id,
  channel_id,
  CAST(update_time AS DATE) AS statistic_time,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(order_no) AS bet_count,
  SUM(IF(status = 'settle', CAST(effective_bet_flag AS BIGINT), 0)) AS effective_bet_count,
  SUM(IF(status = 'settle', 1, 0)) AS settle_count,
  SUM(IF(status = 'settle', bet_amount, 0)) AS settle_bet_amount,
  SUM(bet_amount) AS bet_amount,
  SUM(IF(status = 'settle', settle_amount, 0)) AS settle_amount,
  SUM(IF(status = 'settle', transfer_amount, 0)) AS win_lose_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_amount, 0)) AS effective_bet_amount,
  SUM(bet_deposit_amount) AS bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS bet_bonus_amount,
  SUM(settle_deposit_amount) AS settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS settle_bonus_amount,
  SUM(transfer_deposit_amount) AS win_lose_deposit_amount,
  SUM(transfer_withdrawal_amount) AS win_lose_withdrawal_amount,
  SUM(transfer_bonus_amount) AS win_lose_bonus_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_deposit_amount, 0)) AS effective_bet_deposit_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_withdrawal_amount, 0)) AS effective_bet_withdrawal_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_bonus_amount, 0)) AS effective_bet_bonus_amount
FROM
  kafka_game_record
where
  status = 'settle'
GROUP BY
  CAST(update_time AS DATE),
  channel_id,
  game_category_id;

-- ==========================================
-- 2. 渠道 + 游戏厂商聚合
-- ==========================================
INSERT INTO
  polar_channel_provider_statistic
SELECT
  channel_id,
  CAST(update_time AS DATE) AS statistic_time,
  third_platform_provider_type,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(order_no) AS bet_count,
  SUM(IF(status = 'settle', 1, 0)) AS settle_count,
  SUM(IF(status = 'settle', CAST(effective_bet_flag AS BIGINT), 0)) AS effective_bet_count,
  SUM(IF (status = 'settle', bet_amount, 0)) AS settle_bet_amount,
  SUM(bet_amount) AS bet_amount,
  SUM(IF(status = 'settle', settle_amount, 0)) AS settle_amount,
  SUM(IF(status = 'settle', transfer_amount, 0)) AS win_lose_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_amount,  0)) AS effective_bet_amount,
  SUM(bet_deposit_amount) AS bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS bet_bonus_amount,
  SUM(settle_deposit_amount) AS settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS settle_bonus_amount,
  SUM(transfer_deposit_amount) AS win_lose_deposit_amount,
  SUM(transfer_withdrawal_amount) AS win_lose_withdrawal_amount,
  SUM(transfer_bonus_amount) AS win_lose_bonus_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_deposit_amount, 0)) AS effective_bet_deposit_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_withdrawal_amount, 0)) AS effective_bet_withdrawal_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_bonus_amount, 0)) AS effective_bet_bonus_amount
FROM
  kafka_game_record
where
  status = 'settle'
GROUP BY
  CAST(update_time AS DATE),
  channel_id,
  third_platform_provider_type;

-- ==========================================
-- 3. 渠道纯维度聚合 (不带厂商和分类)
-- ==========================================
INSERT INTO
  polar_channel_record_statistic
SELECT
  channel_id,
  CAST(update_time AS DATE) AS statistic_time,
  SUM(IF (status = 'settle', 1, 0)) AS settle_count,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(order_no) AS bet_count,
  SUM(IF(status = 'settle', CAST(effective_bet_flag AS BIGINT), 0)) AS effective_bet_count,
  SUM(IF(status = 'settle', bet_amount, 0)) AS settle_bet_amount,
  SUM(bet_amount) AS bet_amount,
  SUM(IF(status = 'settle', settle_amount, 0)) AS settle_amount,
  SUM(IF(status = 'settle', transfer_amount, 0)) AS win_lose_amount,
  SUM(IF(status = 'settle'  AND effective_bet_flag = 1, valid_bet_amount, 0)) AS effective_bet_amount,
  SUM(bet_deposit_amount) AS bet_deposit_amount,
  SUM(bet_withdrawal_amount) AS bet_withdrawal_amount,
  SUM(bet_bonus_amount) AS bet_bonus_amount,
  SUM(settle_deposit_amount) AS settle_deposit_amount,
  SUM(settle_withdrawal_amount) AS settle_withdrawal_amount,
  SUM(settle_bonus_amount) AS settle_bonus_amount,
  SUM(transfer_deposit_amount) AS win_lose_deposit_amount,
  SUM(transfer_withdrawal_amount) AS win_lose_withdrawal_amount,
  SUM(transfer_bonus_amount) AS win_lose_bonus_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_deposit_amount, 0)) AS effective_bet_deposit_amount,
  SUM(IF(status = 'settle' AND effective_bet_flag = 1, valid_bet_withdrawal_amount, 0)) AS effective_bet_withdrawal_amount,
  SUM(IF(status = 'settle'  AND effective_bet_flag = 1, valid_bet_bonus_amount, 0)) AS effective_bet_bonus_amount
FROM
  kafka_game_record
where
  status = 'settle'
GROUP BY
  CAST(update_time AS DATE),
  channel_id;
