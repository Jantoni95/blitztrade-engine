param(
    [string]$Python = "py -3.12",
    [string]$VenvName = ".build-venv"
)

$ErrorActionPreference = "Stop"

$venvPath = Join-Path $PSScriptRoot $VenvName

Write-Host "Creating isolated build venv at $venvPath"
Invoke-Expression "$Python -m venv `"$venvPath`""

$pythonExe = Join-Path $venvPath "Scripts\python.exe"

& $pythonExe -m pip install --upgrade pip setuptools wheel
& $pythonExe -m pip install -r (Join-Path $PSScriptRoot "requirements-build.txt")

Write-Host ""
Write-Host "Build environment ready."
Write-Host "Use:"
Write-Host "  $pythonExe build_app.py --version 1.1.9 --windows-console"