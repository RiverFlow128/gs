-- ADS 层所有表的汇总文件
-- 用于 Dinky 平台批量执行

-- 1. 今日用户奖金统计
@include 'today_user_bounty_statistic.sql';

-- 2. 今日用户游戏统计
@include 'today_user_game_statistic.sql';

-- 3. 今日用户奖励统计
@include 'today_user_reward_statistic.sql';
