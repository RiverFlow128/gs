-- ==========================================
-- DWS层：奖励记录聚合统计
-- 职责：从DWD层读取清洗后的数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_reward_record (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: reward_record, today_user_reward_statistics_dws,
--        today_channel_reward_statistic, today_channel_activity_statistic (PolarDB)
--
-- 上游表：dwd_reward_record (Kafka)
-- 上游文件：dwd/reward_record_dwd.sql
-- 下游表：reward_record, today_user_reward_statistics_dws,
--         today_channel_reward_statistic, today_channel_activity_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- =========== 1. 全局资源与稳定性配置 ===========
SET 'execution.checkpointing.interval' = '3min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE'; -- 设置模式为 EXACTLY_ONCE
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
-- 必须修改为分布式文件系统路径 - 已清理旧集群
--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';
-- 建议开启增量 Checkpoint，在 50 万 QPS 下的性能同步压力SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION'; -- 确保 Checkpoint 在任务取消时保留，以便重启恢复
-- 生产环境必须配置分布式存储路径，本地文件路径在集群模式下无法恢复
-- SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 原配置（仅用于开发）
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 修改为：使用OSS分布式存储
-- 统一时区配置为 Singapore 时区
-- SET 'table.local-time-zone' = 'Asia/Jakarta'; -- 原配置
SET 'table.local-time-zone' = 'Asia/Singapore'; -- 修改为：使用Singapore时区

-- =========== 2. 性能优化（应对高并发写入 PolarDB）===========
SET 'table.exec.mini-batch.enabled' = 'true'; -- 开启批处理，减少对数据库的访问频率
-- 批处理延迟应该 ≤ Checkpoint间隔/2，避免checkpoint触发时积压未提交数据
-- SET 'table.exec.mini-batch.allow-latency' = '3min'; -- 原配置（与checkpoint冲突）
SET 'table.exec.mini-batch.allow-latency' = '1min'; -- 修改为：1分钟 ≤ 3分钟Checkpoint间隔/2
SET 'table.exec.mini-batch.size' = '10000'; -- 开启批处理，减少对数据库的访问频率
SET 'table.exec.state.ttl' = '36h'; -- 开启状态清理，确保内存不会被历史数据撑爆（针对天级表，设置 36 小时足够）
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE'; -- 开启全局聚合优化，解决高并发下的 COUNT DISTINCT 性能问题
SET 'table.optimizer.distinct-agg.split.enabled' = 'true'; -- 开启去重聚合优化（针对你那几个 COUNT DISTINCT）

-- 2. 定义DWD层源表(从DWD层Kafka读取清洗后的数据)
CREATE TABLE dwd_reward_record (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  activity_type STRING,
  id BIGINT,
  unique_id BIGINT,
  reward_scope STRING,
  reward_type STRING,
  bonus_amount DECIMAL(20, 4),
  draw_flag INT,
  not_receive_time TIMESTAMP(3),
  receive_time TIMESTAMP(3),
  time_out_time TIMESTAMP(3),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  WATERMARK FOR update_time AS update_time - INTERVAL '5' MINUTE,
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_reward_record',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'dws_reward_record_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);


create table target_reward_record
(
    id               bigint          comment '主键ID',
    user_id          bigint          comment '用户ID',
    channel_id       bigint          comment '渠道ID',
    unique_id        bigint          comment '奖励记录唯一编码',
    reward_scope     STRING          comment '奖励范围',
    reward_type      STRING          comment '奖励类型（临时用，不涉及敏感字符）',
    activity_type    STRING          comment '活动类型:  OTHER-其他,REFERRAL_REWARD-推荐奖励...',
    bonus_amount     decimal(20, 4)  comment '奖励金额：本次奖励的金额',
    -- 与生产环境保持一致，使用 INT 类型 (0: 未领取, 1: 已领取)
    draw_flag        INT         comment '领取状态 (0: 未领取, 1: 已领取)',
    not_receive_time TIMESTAMP(3)        comment '奖励逾期未领取记录时间',
    receive_time     TIMESTAMP(3)        comment '奖励领取时间',
    time_out_time    TIMESTAMP(3)        comment '过期时间',
    create_time      TIMESTAMP(3)        comment '创建时间',
    update_time      TIMESTAMP(3)        comment '更新时间',
    primary key (user_id, unique_id, create_time) NOT ENFORCED
)
WITH
    (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'reward_record',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    
    -- 1. 开启批量写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 解决方案：在 INSERT 语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

    );






-- TODO: 修改原因 - 注释与实际配置不符，这是MySQL而非PolarDB
-- 定义 MySQL 目标表（注：虽然要求说PolarDB，实际连接为MySQL）
create table today_user_reward_statistics_dws
(
    -- 统计维度和标识
    statistic_time DATE NOT NULL comment '统计日期',
    user_id BIGINT NOT NULL comment '用户ID',
    channel_id BIGINT NOT NULL comment '渠道ID',
    activity_type STRING NOT NULL comment '活动类型（主要用于区分原始活动表）',

    -- VIP系统统计
    vip_draw_count BIGINT comment 'VIP系统领取次数',
    vip_no_draw_count BIGINT comment 'VIP系统待领取数量',
    vip_draw_amount DECIMAL(20, 4) comment 'VIP系统领取金额',
    vip_no_draw_amount DECIMAL(20, 4) comment 'VIP系统待领取金额',
    vip_draw_total_withdrawal_amount DECIMAL(20, 4) comment 'VIP系统-总金额-可提取金额',
    vip_draw_total_deposit_amount DECIMAL(20, 4) comment 'VIP系统-总金额-固定金额',
    vip_draw_total_bonus_amount DECIMAL(20, 4) comment 'VIP系统-总金额-奖励',
    vip_draw_withdrawal_amount DECIMAL(20, 4) comment 'VIP系统-已领取-可提取金额',
    vip_draw_deposit_amount DECIMAL(20, 4) comment 'VIP系统-已领取-固定金额',
    vip_draw_bonus_amount DECIMAL(20, 4) comment 'VIP系统-已领取-奖励',
    vip_no_draw_withdrawal_amount DECIMAL(20, 4) comment 'VIP系统-待领取-可提取金额',
    vip_no_draw_deposit_amount DECIMAL(20, 4) comment 'VIP系统-待领取-固定金额',
    vip_no_draw_bonus_amount DECIMAL(20, 4) comment 'VIP系统-待领取-奖励',

    -- 返佣系统统计
    rebate_draw_count BIGINT comment '返佣领取次数',
    rebate_no_draw_count BIGINT comment '返佣待领取数量',
    rebate_draw_amount DECIMAL(20, 4) comment '返佣领取金额',
    rebate_no_draw_amount DECIMAL(20, 4) comment '返佣待领取金额',
    rebate_draw_total_withdrawal_amount DECIMAL(20, 4) comment '返佣-总金额-可提取金额',
    rebate_draw_total_deposit_amount DECIMAL(20, 4) comment '返佣-总金额-固定金额',
    rebate_draw_total_bonus_amount DECIMAL(20, 4) comment '返佣-总金额-奖励',
    rebate_draw_withdrawal_amount DECIMAL(20, 4) comment '返佣-已领取-可提取金额',
    rebate_draw_deposit_amount DECIMAL(20, 4) comment '返佣-已领取-固定金额',
    rebate_draw_bonus_amount DECIMAL(20, 4) comment '返佣-已领取-奖励',
    rebate_no_draw_withdrawal_amount DECIMAL(20, 4) comment '返佣待领取-可提取金额',
    rebate_no_draw_deposit_amount DECIMAL(20, 4) comment '返佣待领取-固定金额',
    rebate_no_draw_bonus_amount DECIMAL(20, 4) comment '返佣待领取-奖励',

    -- 活动统计
    activity_total_count BIGINT comment '活动总数量',
    activity_draw_count BIGINT comment '活动领取次数',
    activity_no_draw_count BIGINT comment '活动待领取数量',
    activity_total_amount DECIMAL(20, 4) comment '活动总金额',
    activity_draw_amount DECIMAL(20, 4) comment '活动领取金额',
    activity_no_draw_amount DECIMAL(20, 4) comment '活动待领取金额',
    activity_total_withdrawal_amount DECIMAL(20, 4) comment '活动奖励总金额-可提取',
    activity_total_deposit_amount DECIMAL(20, 4) comment '活动奖励总金额-固定',
    activity_total_bonus_amount DECIMAL(20, 4) comment '活动奖励总金额-奖励',
    activity_draw_withdrawal_amount DECIMAL(20, 4) comment '活动已领取-可提取',
    activity_draw_deposit_amount DECIMAL(20, 4) comment '活动已领取-固定',
    activity_draw_bonus_amount DECIMAL(20, 4) comment '活动已领取-奖励',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) comment '活动未领取-可提取',
    activity_no_draw_deposit_amount DECIMAL(20, 4) comment '活动未领取-固定',
    activity_no_draw_bonus_amount DECIMAL(20, 4) comment '活动未领取-奖励',

    -- 奖励任务统计
    bonus_task_draw_count BIGINT comment '奖励任务领取次数',
    bonus_task_no_draw_count BIGINT comment '奖励任务待领取数量',
    bonus_task_draw_amount DECIMAL(20, 4) comment '奖励任务领取金额',
    bonus_task_no_draw_amount DECIMAL(20, 4) comment '奖励任务待领取金额',
    bonus_task_total_withdrawal_amount DECIMAL(20, 4) comment '奖励任务-总金额-可提取金额',
    bonus_task_total_deposit_amount DECIMAL(20, 4) comment '奖励任务-总金额-固定金额',
    bonus_task_total_bonus_amount DECIMAL(20, 4) comment '奖励任务-总金额-奖励',
    bonus_task_no_draw_withdrawal_amount DECIMAL(20, 4) comment '奖励任务-待领取-可提取金额',
    bonus_task_no_draw_deposit_amount DECIMAL(20, 4) comment '奖励任务-待领取-固定金额',
    bonus_task_no_draw_bonus_amount DECIMAL(20, 4) comment '奖励任务-待领取-奖励',
    bonus_task_draw_withdrawal_amount DECIMAL(20, 4) comment '奖励任务-已领取-可提取金额',
    bonus_task_draw_deposit_amount DECIMAL(20, 4) comment '奖励任务-已领取-固定金额',
    bonus_task_draw_bonus_amount DECIMAL(20, 4) comment '奖励任务-已领取-奖励',

    -- 游客体验金统计
    tourist_draw_count BIGINT comment '游客体验金领取数量',
    tourist_no_draw_count BIGINT comment '游客体验金待领取次数',
    tourist_draw_total_withdrawal_amount DECIMAL(20, 4) comment '游客体验金-总金额-可提取金额',
    tourist_draw_total_deposit_amount DECIMAL(20, 4) comment '游客体验金-总金额-固定金额',
    tourist_draw_total_bonus_amount DECIMAL(20, 4) comment '游客体验金-总金额-奖励',
    tourist_draw_amount DECIMAL(20, 4) comment '游客体验金已领取金额',
    tourist_draw_withdrawal_amount DECIMAL(20, 4) comment '游客体验金-已领取-可提取金额',
    tourist_draw_deposit_amount DECIMAL(20, 4) comment '游客体验金-已领取-固定金额',
    tourist_draw_bonus_amount DECIMAL(20, 4) comment '游客体验金-已领取-奖励',
    tourist_no_draw_amount DECIMAL(20, 4) comment '游客体验金-待领取-金额',
    tourist_no_draw_withdrawal_amount DECIMAL(20, 4) comment '游客体验金-待领取-可提取金额',
    tourist_no_draw_deposit_amount DECIMAL(20, 4) comment '游客体验金-待领取-固定金额',
    tourist_no_draw_bonus_amount DECIMAL(20, 4) comment '游客体验金-待领取-奖励',

    -- 主键定义（NOT ENFORCED表示不强制检查，实际检查由目标数据库完成）
    PRIMARY KEY (statistic_time, user_id, channel_id, activity_type) NOT ENFORCED
)
WITH
    (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_reward_statistics_dws',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    
    -- 1. 开启批量写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 解决方案：在 INSERT 语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

    );
-- 渠道目标表
CREATE TABLE today_channel_reward_statistic (
    user_count BIGINT COMMENT '用户总数量',
    vip_user_count BIGINT COMMENT 'VIP奖励参与人数',

    -- VIP系统统计
    vip_draw_count BIGINT COMMENT 'VIP系统领取次数',
    vip_no_draw_count BIGINT COMMENT 'VIP系统待领取数量',
    vip_draw_amount DECIMAL(20, 4) COMMENT 'VIP系统领取金额',
    vip_no_draw_amount DECIMAL(20, 4) COMMENT 'VIP系统待领取金额',
    vip_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP系统-总金额-可提取金额',
    vip_draw_total_deposit_amount DECIMAL(20, 4) COMMENT 'VIP系统-总金额-固定金额',
    vip_draw_total_bonus_amount DECIMAL(20, 4) COMMENT 'VIP系统-总金额-奖励',
    vip_draw_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP系统-已领取-可提取金额',
    vip_draw_deposit_amount DECIMAL(20, 4) COMMENT 'VIP系统-已领取-固定金额',
    vip_draw_bonus_amount DECIMAL(20, 4) COMMENT 'VIP系统-已领取-奖励',
    vip_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP系统-待领取-可提取金额',
    vip_no_draw_deposit_amount DECIMAL(20, 4) COMMENT 'VIP系统-待领取-固定金额',
    vip_no_draw_bonus_amount DECIMAL(20, 4) COMMENT 'VIP系统-待领取-奖励',

    -- 返佣系统统计
    rebate_user_count BIGINT COMMENT '返佣奖励参与人数',
    rebate_draw_count BIGINT COMMENT '返佣领取次数',
    rebate_no_draw_count BIGINT COMMENT '返佣待领取数量',
    rebate_draw_amount DECIMAL(20, 4) COMMENT '返佣领取金额',
    rebate_no_draw_amount DECIMAL(20, 4) COMMENT '返佣待领取金额',
    rebate_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT '返佣-总金额-可提取金额',
    rebate_draw_total_deposit_amount DECIMAL(20, 4) COMMENT '返佣-总金额-固定金额',
    rebate_draw_total_bonus_amount DECIMAL(20, 4) COMMENT '返佣-总金额-奖励',
    rebate_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '返佣-已领取-可提取金额',
    rebate_draw_deposit_amount DECIMAL(20, 4) COMMENT '返佣-已领取-固定金额',
    rebate_draw_bonus_amount DECIMAL(20, 4) COMMENT '返佣-已领取-奖励',
    rebate_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '返佣待领取-可提取金额',
    rebate_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '返佣待领取-固定金额',
    rebate_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '返佣待领取-奖励',

    -- 活动统计
    activity_user_count BIGINT COMMENT '活动奖励参与人数',
    activity_draw_count BIGINT COMMENT '活动领取次数',
    activity_no_draw_count BIGINT COMMENT '活动待领取数量',
    activity_total_amount DECIMAL(20, 4) COMMENT '活动奖励-总金额',
    activity_draw_amount DECIMAL(20, 4) COMMENT '活动领取金额',
    activity_no_draw_amount DECIMAL(20, 4) COMMENT '活动待领取金额',
    activity_total_withdrawal_amount DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-可提取金额',
    activity_total_deposit_amount DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-固定金额',
    activity_total_bonus_amount DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-奖励',
    activity_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-可提取金额',
    activity_draw_deposit_amount DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-固定金额',
    activity_draw_bonus_amount DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-奖励',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-可提取金额',
    activity_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-固定金额',
    activity_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-奖励',

    -- 奖励任务统计
    bonus_task_user_count BIGINT COMMENT '奖励任务参与人数',
    bonus_task_draw_count BIGINT COMMENT '奖励任务领取次数',
    bonus_task_no_draw_count BIGINT COMMENT '奖励任务待领取数量',
    bonus_task_draw_amount DECIMAL(20, 4) COMMENT '奖励任务领取金额',
    bonus_task_no_draw_amount DECIMAL(20, 4) COMMENT '奖励任务待领取金额',
    bonus_task_total_withdrawal_amount DECIMAL(20, 4) COMMENT '奖励任务-总金额-可提取金额',
    bonus_task_total_deposit_amount DECIMAL(20, 4) COMMENT '奖励任务-总金额-固定金额',
    bonus_task_total_bonus_amount DECIMAL(20, 4) COMMENT '奖励任务-总金额-奖励',
    bonus_task_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '奖励任务-待领取-可提取金额',
    bonus_task_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '奖励任务-待领取-固定金额',
    bonus_task_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '奖励任务-待领取-奖励',
    bonus_task_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '奖励任务-已领取-可提取金额',
    bonus_task_draw_deposit_amount DECIMAL(20, 4) COMMENT '奖励任务-已领取-固定金额',
    bonus_task_draw_bonus_amount DECIMAL(20, 4) COMMENT '奖励任务-已领取-奖励',

    -- 维度字段
    channel_id BIGINT NOT NULL COMMENT '渠道ID',
    statistic_time DATE NOT NULL COMMENT '统计日期',

    -- 游客体验金统计
    tourist_draw_count BIGINT COMMENT '游客体验金领取数量',
    tourist_no_draw_count BIGINT COMMENT '游客体验金待领取次数',
    tourist_draw_amount DECIMAL(20, 4) COMMENT '游客体验金领取金额',
    tourist_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT '游客体验金-总金额-可提取金额',
    tourist_draw_total_deposit_amount DECIMAL(20, 4) COMMENT '游客体验金-总金额-固定金额',
    tourist_draw_total_bonus_amount DECIMAL(20, 4) COMMENT '游客体验金-总金额-奖励',
    tourist_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '游客体验金-已领取-可提取金额',
    tourist_draw_deposit_amount DECIMAL(20, 4) COMMENT '游客体验金-已领取-固定金额',
    tourist_draw_bonus_amount DECIMAL(20, 4) COMMENT '游客体验金-已领取-奖励',
    tourist_no_draw_amount DECIMAL(20, 4) COMMENT '游客体验金-待领取-金额',
    tourist_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '游客体验金-待领取-可提取金额',
    tourist_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '游客体验金-待领取-固定金额',
    tourist_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '游客体验金-待领取-奖励',

    -- 主键定义
    PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
)
COMMENT '渠道今日奖励统计日报'
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_reward_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    
    -- 1. 开启批量写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 解决方案：在 INSERT 语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

);



-- 目标表：渠道活动统计表
CREATE TABLE today_channel_activity_statistic
(
    user_count                         BIGINT COMMENT '用户总数量',
    channel_id                         BIGINT NOT NULL COMMENT '渠道ID',
    activity_type                      STRING NOT NULL COMMENT '活动类型（对应枚举 ActivityTypeEnum）',
    statistic_time                     DATE   NOT NULL COMMENT '统计日期',
    view_activity_count                BIGINT COMMENT '查看活动人数',
    activity_reward_count              BIGINT COMMENT '获得活动奖励人数',
    activity_total_amount              DECIMAL(20, 4) COMMENT '活动总金额',
    activity_user_count                BIGINT COMMENT '活动的人数',
    activity_draw_count                BIGINT COMMENT '活动领取次数',
    activity_no_draw_count             BIGINT COMMENT '活动待领取数量',
    activity_draw_amount               DECIMAL(20, 4) COMMENT '活动领取金额',
    activity_no_draw_amount            DECIMAL(20, 4) COMMENT '活动待领取金额',
    activity_total_withdrawal_amount   DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-可提取金额',
    activity_total_deposit_amount      DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-固定金额',
    activity_total_bonus_amount        DECIMAL(20, 4) COMMENT '活动奖励金额-总金额-奖励',
    activity_draw_withdrawal_amount    DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-可提取金额',
    activity_draw_deposit_amount       DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-固定金额',
    activity_draw_bonus_amount         DECIMAL(20, 4) COMMENT '活动奖励金额-已领取-奖励',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-可提取金额',
    activity_no_draw_deposit_amount    DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-固定金额',
    activity_no_draw_bonus_amount      DECIMAL(20, 4) COMMENT '活动奖励金额-未领取-奖励',

    -- 主键定义（和MySQL表一致）
    PRIMARY KEY (statistic_time, activity_type, channel_id) NOT ENFORCED
) COMMENT '渠道活动日统计表'
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_activity_statistic',
    'username' = 'super',
    'password' = '${MYSQL_PASSWORD}',
    
    -- 1. 开启批量写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 解决方案：在 INSERT 语句中使用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等写入

);




-- 目标表插入：奖励记录明细表 INSERT INTO target_reward_record
SELECT
    id,
    user_id,
    channel_id,
    unique_id,
    reward_scope,
    reward_type,
    activity_type,
    bonus_amount,
    draw_flag,
    not_receive_time,
    receive_time,
    time_out_time,
    create_time,
    update_time
FROM dwd_reward_record
WHERE id IS NOT NULL;

-- 目标表插入：用户维度奖励统计表
INSERT INTO today_user_reward_statistics_dws
SELECT
    statistic_time,
    user_id,
    channel_id,
    COALESCE(activity_type, '') AS activity_type,

    -- ============ VIP系统统计 ============
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN 1 END) AS vip_draw_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN 1 END) AS vip_no_draw_count,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS vip_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS vip_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_no_draw_bonus_amount,

    -- ============ 返佣系统统计 ============
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN 1 END) AS rebate_draw_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN 1 END) AS rebate_no_draw_count,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS rebate_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS rebate_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_bonus_amount,

    -- ============ 活动统计 (此处通过内部去重合并并汇总) ============
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_total_count,
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_draw_count,
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_no_draw_count,
    
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN bonus_amount ELSE 0 END) AS activity_total_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount,

    -- ============ 奖励任务统计 ============
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 END) AS bonus_task_draw_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 END) AS bonus_task_no_draw_count,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS bonus_task_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_bonus_amount,

    -- ============ 游客体验金统计 ============
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN 1 END) AS tourist_draw_count,
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN 1 END) AS tourist_no_draw_count,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS tourist_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS tourist_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY statistic_time, user_id, channel_id, COALESCE(activity_type, '');



-- 目标表插入：渠道维度奖励统计表
INSERT INTO today_channel_reward_statistic
SELECT
    -- 1. 用户去重总数量
    COUNT(DISTINCT user_id) AS user_count,

    -- 2. VIP系统统计
    COUNT(DISTINCT CASE WHEN reward_scope = 'VIP_REWARD' THEN user_id END) AS vip_user_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN 1 END) AS vip_draw_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN 1 END) AS vip_no_draw_count,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS vip_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS vip_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_no_draw_bonus_amount,

    -- 3. 返佣系统统计
    COUNT(DISTINCT CASE WHEN reward_scope = 'REBATE_REWARD' THEN user_id END) AS rebate_user_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN 1 END) AS rebate_draw_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN 1 END) AS rebate_no_draw_count,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS rebate_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS rebate_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_withdrawal_amount,

    -- 4. 活动系统统计 (去除 Join 简化，直接聚合)
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN user_id END) AS activity_user_count,
    COUNT(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN 1 END) AS activity_draw_count,
    COUNT(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN 1 END) AS activity_no_draw_count,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN bonus_amount ELSE 0 END) AS activity_total_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount,

    -- 5. 奖励任务统计
    COUNT(DISTINCT CASE WHEN reward_scope = 'BONUS_TASK' THEN user_id END) AS bonus_task_user_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 END) AS bonus_task_draw_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 END) AS bonus_task_no_draw_count,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS bonus_task_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_bonus_amount,

    -- 6. 基础维度 (设置在目标表指定位置)
    channel_id,
    statistic_time,

    -- 7. 游客体验金统计
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN 1 END) AS tourist_draw_count,
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN 1 END) AS tourist_no_draw_count,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS tourist_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS tourist_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY statistic_time, channel_id;



-- 目标表插入：渠道维度活动统计表
INSERT INTO today_channel_activity_statistic
SELECT
    -- 1. 用户统计 (由原 user_stats 合并而来)
    COUNT(DISTINCT user_id) AS user_count,
    
    -- 2. 基础维度
    channel_id,
    activity_type,
    statistic_time,
    
    -- 3. 固定占位符
    0 AS view_activity_count,
    
    -- 4. 活动次数去重统计 (由原 activity_stats 合并而来)
    COUNT(DISTINCT CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) AS activity_reward_count,
    
    -- 5. 金额汇总统计 (由原 main_stats 合并而来)
    SUM(bonus_amount) AS activity_total_amount,
    
    -- 6. 活动用户数 (汇总等同于 user_count)
    COUNT(DISTINCT user_id) AS activity_user_count,
    
    -- 7. 已用与未用去重统计
    COUNT(DISTINCT CASE WHEN draw_flag = 1 THEN CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR) END) AS activity_draw_count,
    COUNT(DISTINCT CASE WHEN draw_flag = 0 THEN CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR) END) AS activity_no_draw_count,
    
    -- 8. 分组状态金额统计
    SUM(CASE WHEN draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    
    -- 9. 分组奖励类型金额统计
    SUM(CASE WHEN reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    
    -- 10. 已用状态下的细分奖励类型
    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    
    -- 11. 未领取状态下的细分奖励类型
    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND activity_type IS NOT NULL
  AND reward_scope = 'ACTIVITY_REWARD'
GROUP BY statistic_time, channel_id, activity_type;





