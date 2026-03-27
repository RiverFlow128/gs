-- DWS 层所有表的汇总文件
-- 用于 Dinky 平台批量执行

-- 1. 后台金额操作记录 DWS
@include 'backend_amount_operation_record_dws_to_db.sql';

-- 2. 游戏记录 DWS
@include 'game_record_dws_to_db.sql';

-- 3. 充值订单 DWS
@include 'recharge_order_dws_to_db.sql';

-- 4. 奖励记录 DWS
@include 'reward_record_dws_to_db.sql';

-- 5. 用户代理统计 DWS
@include 'user_proxy_statistic_dws_to_db.sql';

-- 6. 提现订单 DWS
@include 'withdraw_order_dws_to_db.sql';
