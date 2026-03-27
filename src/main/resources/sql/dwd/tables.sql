-- DWD 层所有表的汇总文件
-- 用于 Dinky 平台批量执行

-- 1. 金额操作记录 DWD
@include 'amount_operation_record_dwd.sql';

-- 2. 游戏记录 DWD
@include 'game_record_dwd.sql';

-- 3. 用户在线状态记录
@include 'kafka_to_user_online_status_record.sql';

-- 4. 充值订单 DWD
@include 'recharge_order_dwd.sql';

-- 5. 奖励记录 DWD
@include 'reward_record_dwd.sql';

-- 6. 用户代理统计 DWD
@include 'user_proxy_statistic_dwd.sql';

-- 7. 提现订单 DWD
@include 'withdraw_order_dwd.sql';
