-- ODS 层所有表的汇总文件
-- 用于 Dinky 平台批量执行

-- 1. 后台金额操作记录
@include 'backend_amount_operation_record_to_kafka.sql';

-- 2. 游戏记录
@include 'game_record_to_kafka.sql';

-- 3. 充值订单
@include 'recharge_order_ods_to_kafka.sql';

-- 4. 用户代理统计
@include 'user_proxy_statistic_to_kafka.sql';

-- 5. 提现订单
@include 'withdraw_order_ods_to_kafka.sql';
