# 启动 NL2SQL 服务
# 使用 uvicorn 启动 FastAPI 应用

# 设置 PowerShell 输出编码为 UTF-8（修复中文乱码问题）
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$serviceDir = Split-Path -Parent $scriptDir

Push-Location $serviceDir

try {
    Write-Host "正在启动 NL2SQL 服务..." -ForegroundColor Green
    Write-Host "服务地址: http://127.0.0.1:8000" -ForegroundColor Cyan
    Write-Host "按 Ctrl+C 停止服务" -ForegroundColor Yellow
    Write-Host ""
    
    # 启动 uvicorn 服务器
    uvicorn main:app --host 127.0.0.1 --port 8000 --reload
} catch {
    Write-Host "启动服务失败: $_" -ForegroundColor Red
    Write-Host "请确保已安装所有依赖: pip install -r requirements.txt" -ForegroundColor Yellow
} finally {
    Pop-Location
}
