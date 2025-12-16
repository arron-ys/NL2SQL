# Live 测试集合（真实外部服务）
# 运行所有 live 标记的测试

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    pytest tests/live/ -m "live" -v
} finally {
    Pop-Location
}

