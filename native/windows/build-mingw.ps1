$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$output = Join-Path $root "build\sparecycle-process-manager.exe"
$source = Join-Path $PSScriptRoot "process_manager.c"
New-Item -ItemType Directory -Force -Path (Join-Path $root "build") | Out-Null
gcc -std=c11 -O2 -Wall -Wextra $source -lpsapi -o $output
if ($LASTEXITCODE -ne 0) {
    throw "MinGW build failed with exit code $LASTEXITCODE"
}
Write-Host "Built $output"
