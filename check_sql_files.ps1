# 检查 SQL 文件完整性 PowerShell 脚本

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Realtime Warehouse SQL File Checker" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$basePath = "src/main/resources/sql"
$totalFiles = 0
$existingFiles = 0

# ODS Layer
Write-Host "[ODS Layer] Checking..." -ForegroundColor Yellow
$odsFiles = @(
    "tables.sql",
    "backend_amount_operation_record_to_kafka.sql",
    "game_record_to_kafka.sql",
    "recharge_order_ods_to_kafka.sql",
    "user_proxy_statistic_to_kafka.sql",
    "withdraw_order_ods_to_kafka.sql"
)

foreach ($file in $odsFiles) {
    $path = Join-Path $basePath "ods/$file"
    $totalFiles++
    if (Test-Path $path) {
        Write-Host "  OK: $file" -ForegroundColor Green
        $existingFiles++
    } else {
        Write-Host "  MISSING: $file" -ForegroundColor Red
    }
}

# DIM Layer
Write-Host ""
Write-Host "[DIM Layer] Checking..." -ForegroundColor Yellow
$dimFiles = @(
    "tables.sql",
    "user_cash_account_to_db.sql",
    "user_global_currency_account_to_db.sql",
    "user_info_to_db.sql",
    "user_online_status_record_to_db.sql",
    "user_proxy_account_to_db.sql",
    "user_proxy_statistic_to_db.sql",
    "user_relations_to_db.sql",
    "user_to_db.sql"
)

foreach ($file in $dimFiles) {
    $path = Join-Path $basePath "dim/$file"
    $totalFiles++
    if (Test-Path $path) {
        Write-Host "  OK: $file" -ForegroundColor Green
        $existingFiles++
    } else {
        Write-Host "  MISSING: $file" -ForegroundColor Red
    }
}

# DWD Layer
Write-Host ""
Write-Host "[DWD Layer] Checking..." -ForegroundColor Yellow
$dwdFiles = @(
    "tables.sql",
    "amount_operation_record_dwd.sql",
    "game_record_dwd.sql",
    "kafka_to_user_online_status_record.sql",
    "recharge_order_dwd.sql",
    "reward_record_dwd.sql",
    "user_proxy_statistic_dwd.sql",
    "withdraw_order_dwd.sql"
)

foreach ($file in $dwdFiles) {
    $path = Join-Path $basePath "dwd/$file"
    $totalFiles++
    if (Test-Path $path) {
        Write-Host "  OK: $file" -ForegroundColor Green
        $existingFiles++
    } else {
        Write-Host "  MISSING: $file" -ForegroundColor Red
    }
}

# DWS Layer
Write-Host ""
Write-Host "[DWS Layer] Checking..." -ForegroundColor Yellow
$dwsFiles = @(
    "tables.sql",
    "backend_amount_operation_record_dws_to_db.sql",
    "game_record_dws_to_db.sql",
    "recharge_order_dws_to_db.sql",
    "reward_record_dws_to_db.sql",
    "user_proxy_statistic_dws_to_db.sql",
    "withdraw_order_dws_to_db.sql"
)

foreach ($file in $dwsFiles) {
    $path = Join-Path $basePath "dws/$file"
    $totalFiles++
    if (Test-Path $path) {
        Write-Host "  OK: $file" -ForegroundColor Green
        $existingFiles++
    } else {
        Write-Host "  MISSING: $file" -ForegroundColor Red
    }
}

# ADS Layer
Write-Host ""
Write-Host "[ADS Layer] Checking..." -ForegroundColor Yellow
$adsFiles = @(
    "tables.sql",
    "today_user_bounty_statistic.sql",
    "today_user_game_statistic.sql",
    "today_user_reward_statistic.sql"
)

foreach ($file in $adsFiles) {
    $path = Join-Path $basePath "ads/$file"
    $totalFiles++
    if (Test-Path $path) {
        Write-Host "  OK: $file" -ForegroundColor Green
        $existingFiles++
    } else {
        Write-Host "  MISSING: $file" -ForegroundColor Red
    }
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if ($existingFiles -eq $totalFiles) {
    Write-Host "All files ready ($existingFiles/$totalFiles)" -ForegroundColor Green
    Write-Host ""
    Write-Host "Ready to run!" -ForegroundColor Green
    exit 0
} else {
    $missing = $totalFiles - $existingFiles
    Write-Host "Missing $missing files ($existingFiles/$totalFiles)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please check missing files before running!" -ForegroundColor Red
    exit 1
}
