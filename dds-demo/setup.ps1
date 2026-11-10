# Instalación para Windows 10/11. Uso (PowerShell, dentro de la carpeta dds-demo):
#   powershell -ExecutionPolicy Bypass -File setup.ps1
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$check = "import sys; sys.exit(0 if (3,10) <= sys.version_info[:2] <= (3,13) else 1)"

# cyclonedds solo publica binarios para Python 3.10 - 3.13
$exe = $null; $pre = @()
if (Get-Command py -ErrorAction SilentlyContinue) {
    foreach ($v in @("3.12", "3.13", "3.11", "3.10")) {
        & py "-$v" -c $check 2>$null
        if ($LASTEXITCODE -eq 0) { $exe = "py"; $pre = @("-$v"); break }
    }
}
if (-not $exe -and (Get-Command python -ErrorAction SilentlyContinue)) {
    & python -c $check 2>$null
    if ($LASTEXITCODE -eq 0) { $exe = "python" }
}
if (-not $exe) {
    Write-Host "ERROR: hace falta Python entre 3.10 y 3.13 (recomendado 3.12). Ver paso 1 de la guía"
    exit 1
}
& $exe @pre --version

if (Test-Path .venv) { Remove-Item -Recurse -Force .venv }
& $exe @pre -m venv .venv
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR creando el entorno virtual"; exit 1 }

& .\.venv\Scripts\python.exe -m pip install --quiet --upgrade pip
& .\.venv\Scripts\python.exe -m pip install --quiet cyclonedds
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR instalando cyclonedds"; exit 1 }
& .\.venv\Scripts\python.exe check_install.py
Write-Host "Listo. En cada terminal: .venv\Scripts\Activate.ps1"
