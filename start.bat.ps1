# 实时数仓快速启动脚本 (PowerShell)
# 适用于 Windows 环境

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "游戏业务实时数仓 - 快速启动" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. 检查 Java 版本
Write-Host "[1/4] 检查 Java 环境..." -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-String "version"
    Write-Host "✓ Java 已安装：$javaVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误：未找到 Java 环境，请先安装 JDK 11+" -ForegroundColor Red
    exit 1
}

# 2. 检查 Maven
Write-Host "[2/4] 检查 Maven 环境..." -ForegroundColor Yellow
try {
    $mavenVersion = mvn -version 2>&1 | Select-String "Apache Maven"
    Write-Host "✓ Maven 已安装" -ForegroundColor Green
} catch {
    Write-Host "✗ 错误：未找到 Maven，请先安装 Maven 3.6+" -ForegroundColor Red
    exit 1
}

# 3. 编译项目
Write-Host "[3/4] 编译项目..." -ForegroundColor Yellow
mvn clean package -DskipTests
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ 编译失败" -ForegroundColor Red
    exit 1
}
Write-Host "✓ 编译成功" -ForegroundColor Green

# 4. 运行程序
Write-Host "[4/4] 启动实时数仓..." -ForegroundColor Yellow
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "提示：按 Ctrl+C 可停止程序" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 运行主程序
mvn exec:java -Dexec.mainClass="com.example.rtwarehouse.RealtimeWarehouseApplication"
