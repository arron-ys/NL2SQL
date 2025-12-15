# Nightly 测试集合（定时任务）
# 运行所有非 slow 标记的测试

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    pytest tests/ -m "not slow" -v
} finally {
    Pop-Location
}


