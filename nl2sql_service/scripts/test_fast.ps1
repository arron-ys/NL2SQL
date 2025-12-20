# Fast 测试集合（push/PR 触发）
# 运行所有 unit 和 integration 测试

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    $startTime = Get-Date
    pytest tests/ -m "unit or integration" -v
    $endTime = Get-Date
    $duration = $endTime - $startTime
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "Fast suite 执行完成" -ForegroundColor Green
    Write-Host "总耗时: $($duration.TotalSeconds.ToString('F2')) 秒" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
} finally {
    Pop-Location
}


