-- DIM 层所有表的汇总文件
-- 用于 Dinky 平台批量执行

-- 1. 用户现金账户
@include 'user_cash_account_to_db.sql';

-- 2. 用户全球货币账户
@include 'user_global_currency_account_to_db.sql';

-- 3. 用户信息
@include 'user_info_to_db.sql';

-- 4. 用户在线状态记录
@include 'user_online_status_record_to_db.sql';

-- 5. 用户代理账户
@include 'user_proxy_account_to_db.sql';

-- 6. 用户代理统计
@include 'user_proxy_statistic_to_db.sql';

-- 7. 用户关系
@include 'user_relations_to_db.sql';

-- 8. 用户表
@include 'user_to_db.sql';
