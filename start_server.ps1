[CmdletBinding()]
param(
    [string]$BindAddress = "127.0.0.1",
    [int]$BindPort = 8000,
    [switch]$NoReload,
    [ValidateSet("local", "memory", "remote")]
    [string]$VectorStoreMode = "",
    [switch]$CleanCaches,
    [switch]$CleanQdrantInstances
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = $scriptDir
$serviceDir = Join-Path $repoRoot "nl2sql_service"

Push-Location $serviceDir


try {
    $envPath = Join-Path $serviceDir ".env"
    if (-not (Test-Path $envPath)) {
        Write-Host "WARN: .env not found at $envPath (will NOT block startup). Your runtime will rely on existing env vars / defaults." -ForegroundColor Yellow
    }

    if ($VectorStoreMode -ne "") {
        $env:VECTOR_STORE_MODE = $VectorStoreMode
        Write-Host "Using VECTOR_STORE_MODE=$VectorStoreMode (session-only override)" -ForegroundColor Cyan
    }

    if ($CleanCaches) {
        Write-Host "Cleaning caches/build artifacts..." -ForegroundColor Cyan

        $targets = @(
            (Join-Path $repoRoot "__pycache__"),
            (Join-Path $repoRoot ".pytest_cache"),
            (Join-Path $repoRoot ".mypy_cache"),
            (Join-Path $repoRoot ".ruff_cache"),
            (Join-Path $repoRoot "htmlcov"),
            (Join-Path $repoRoot "build"),
            (Join-Path $repoRoot "dist")
        )

        foreach ($t in $targets) {
            if (Test-Path $t) {
                Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $t
            }
        }

        Get-ChildItem -Path $repoRoot -Directory -Recurse -Force -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -eq "__pycache__" } |
            ForEach-Object { Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $_.FullName }

        Get-ChildItem -Path $repoRoot -Filter "*.egg-info" -Directory -Recurse -Force -ErrorAction SilentlyContinue |
            ForEach-Object { Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $_.FullName }

        Get-ChildItem -Path $repoRoot -Filter ".coverage*" -File -Recurse -Force -ErrorAction SilentlyContinue |
            ForEach-Object { Remove-Item -Force -ErrorAction SilentlyContinue $_.FullName }

        Write-Host "Cache cleanup done." -ForegroundColor Green
    }

    if ($CleanQdrantInstances) {
        $qdrantRoot = Join-Path $repoRoot "qdrant_data"
        if (Test-Path $qdrantRoot) {
            Write-Host "Cleaning Qdrant instance_* directories under: $qdrantRoot" -ForegroundColor Cyan

            Get-ChildItem -Path $qdrantRoot -Directory -Force -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -match '^instance_\d+$' } |
                ForEach-Object { Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $_.FullName }

            Write-Host "Qdrant instance_* cleanup done." -ForegroundColor Green
        } else {
            Write-Host "qdrant_data/ not found, skip Qdrant cleanup." -ForegroundColor Yellow
        }
    }

    Write-Host "Starting NL2SQL service..." -ForegroundColor Green
    Write-Host "URL: http://$BindAddress`:$BindPort" -ForegroundColor Cyan
    Write-Host "Ctrl+C to stop" -ForegroundColor Yellow
    Write-Host ""

    if ($NoReload) {
        uvicorn main:app --host $BindAddress --port $BindPort
    } else {
        uvicorn main:app --host $BindAddress --port $BindPort --reload
    }

} catch {
    Write-Host "Failed to start service: $_" -ForegroundColor Red
    Write-Host "Hint: ensure dependencies installed and venv activated." -ForegroundColor Yellow
} finally {
    Pop-Location
}
