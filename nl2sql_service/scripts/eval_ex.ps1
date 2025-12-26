# EX Evaluator CLI Script
# Evaluates NL2SQL execution accuracy against gold dataset

param(
    [string]$DatasetPath = "nl2sql_service/docs/evaluation/dataset/gold_dataset.jsonl",
    [string]$Endpoint = "http://localhost:8000/nl2sql/execute",
    [string]$ResultsDir = "nl2sql_service/docs/evaluation/results",
    [bool]$IncludeTrace = $false,
    [string]$CaseResultsFile = "ex_case_results.jsonl",
    [string]$SummaryFile = "ex_summary.json"
)

# Set error action preference
$ErrorActionPreference = "Stop"

Write-Host "=== EX Evaluator ===" -ForegroundColor Cyan
Write-Host "Dataset: $DatasetPath"
Write-Host "Endpoint: $Endpoint"
Write-Host "Include Trace: $IncludeTrace"
Write-Host ""

# Check if dataset file exists
if (-not (Test-Path $DatasetPath)) {
    Write-Host "ERROR: Dataset file not found: $DatasetPath" -ForegroundColor Red
    exit 1
}

# Create results directory if it doesn't exist
if (-not (Test-Path $ResultsDir)) {
    New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null
    Write-Host "Created results directory: $ResultsDir" -ForegroundColor Green
}

# Full paths for output files
$CaseResultsPath = Join-Path $ResultsDir $CaseResultsFile
$SummaryPath = Join-Path $ResultsDir $SummaryFile

# Clear previous results
if (Test-Path $CaseResultsPath) {
    Remove-Item $CaseResultsPath -Force
}
if (Test-Path $SummaryPath) {
    Remove-Item $SummaryPath -Force
}

# Read dataset
Write-Host "Loading dataset..." -ForegroundColor Yellow
$cases = Get-Content $DatasetPath | ForEach-Object { $_ | ConvertFrom-Json }
Write-Host "Loaded $($cases.Count) cases" -ForegroundColor Green
Write-Host ""

# Collect predictions
Write-Host "Collecting predictions from endpoint..." -ForegroundColor Yellow
$predictions = @()
$caseIndex = 0

foreach ($case in $cases) {
    $caseIndex++
    Write-Host "[$caseIndex/$($cases.Count)] Processing case: $($case.case_id)" -ForegroundColor Cyan
    
    try {
        # Build request body
        $requestBody = @{
            question = $case.question
            include_trace = $IncludeTrace
        } | ConvertTo-Json -Depth 10
        
        # Call API
        $response = Invoke-RestMethod -Uri $Endpoint -Method Post -Body $requestBody -ContentType "application/json" -TimeoutSec 30
        
        # Store prediction
        $predictions += $response
        
        Write-Host "  -> Success" -ForegroundColor Green
    }
    catch {
        Write-Host "  -> ERROR: $($_.Exception.Message)" -ForegroundColor Red
        
        # Store error response
        $errorResponse = @{
            data_list = @()
            error = $_.Exception.Message
        }
        $predictions += $errorResponse
    }
}

Write-Host ""
Write-Host "Predictions collected: $($predictions.Count)" -ForegroundColor Green
Write-Host ""

# Run evaluation using Python
Write-Host "Running EX evaluation..." -ForegroundColor Yellow

# Create temporary Python script
$pythonScript = @"
import json
import sys
from pathlib import Path

# Add nl2sql_service to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from evaluation.ex.evaluator import EXEvaluator
from evaluation.ex.schema import DatasetCase, GoldResult

# Load cases
with open('$($DatasetPath.Replace('\', '/'))') as f:
    cases_data = [json.loads(line) for line in f]

cases = []
for case_data in cases_data:
    gold_result = GoldResult(**case_data['gold_result'])
    case = DatasetCase(
        case_id=case_data['case_id'],
        question=case_data['question'],
        expected_outcome=case_data['expected_outcome'],
        order_sensitive=case_data['order_sensitive'],
        gold_result=gold_result,
        notes=case_data.get('notes')
    )
    cases.append(case)

# Load predictions
with open('temp_predictions.json') as f:
    predictions = json.load(f)

# Run evaluation
evaluator = EXEvaluator()
summary = evaluator.evaluate_dataset(cases, predictions, include_trace=$($IncludeTrace.ToString().ToLower()))

# Write per-case results
with open('$($CaseResultsPath.Replace('\', '/'))', 'w', encoding='utf-8') as f:
    for result in evaluator.results:
        f.write(json.dumps(result.model_dump(), ensure_ascii=False) + '\n')

# Write summary
with open('$($SummaryPath.Replace('\', '/'))', 'w', encoding='utf-8') as f:
    summary_dict = summary.model_dump()
    summary_dict['config']['dataset_path'] = '$($DatasetPath.Replace('\', '/'))'
    summary_dict['config']['endpoint'] = '$Endpoint'
    json.dump(summary_dict, f, indent=2, ensure_ascii=False)

print(f'Total cases: {summary.total_cases}')
print(f'Scorable cases: {summary.scorable_cases}')
print(f'Unscorable cases: {summary.unscorable_cases}')
print(f'Exact match count: {summary.exact_match_count}')
print(f'EX score: {summary.ex_score:.4f}')
print(f'Multi-subquery cases: {summary.multi_subquery_cases}')
"@

# Save predictions to temp file
$predictions | ConvertTo-Json -Depth 100 | Out-File -FilePath "temp_predictions.json" -Encoding utf8

# Save Python script to temp file
$pythonScript | Out-File -FilePath "temp_eval_script.py" -Encoding utf8

# Run Python script
try {
    python temp_eval_script.py
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Python evaluation failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "ERROR: Failed to run Python evaluation: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    # Clean up temp files
    if (Test-Path "temp_predictions.json") {
        Remove-Item "temp_predictions.json" -Force
    }
    if (Test-Path "temp_eval_script.py") {
        Remove-Item "temp_eval_script.py" -Force
    }
}

Write-Host ""
Write-Host "=== Evaluation Complete ===" -ForegroundColor Green
Write-Host "Per-case results: $CaseResultsPath" -ForegroundColor Cyan
Write-Host "Summary report: $SummaryPath" -ForegroundColor Cyan
Write-Host ""

# Display summary
if (Test-Path $SummaryPath) {
    Write-Host "=== Summary ===" -ForegroundColor Cyan
    Get-Content $SummaryPath | ConvertFrom-Json | ConvertTo-Json -Depth 10 | Write-Host
}
