# Full 测试集合（手动触发）
# 运行所有测试（包括 slow 标记的测试）

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    pytest tests/ -v
} finally {
    Pop-Location
}


