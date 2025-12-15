# Fast 测试集合（push/PR 触发）
# 运行所有 unit 和 integration 测试

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    pytest tests/ -m "unit or integration" -v
} finally {
    Pop-Location
}


