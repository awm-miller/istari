param(
    [Parameter(Mandatory = $true)]
    [string]$RepositoryRoot,
    [Parameter(Mandatory = $true)]
    [string]$DatabasePath,
    [Parameter(Mandatory = $true)]
    [string]$StdoutLog,
    [Parameter(Mandatory = $true)]
    [string]$StderrLog,
    [string]$SeedFile = "latest_db_seeds.txt"
)

$ErrorActionPreference = "Stop"

$env:DATABASE_PATH = $DatabasePath
Set-Location $RepositoryRoot

$stdoutDir = Split-Path -Parent $StdoutLog
$stderrDir = Split-Path -Parent $StderrLog
if ($stdoutDir) {
    New-Item -ItemType Directory -Force -Path $stdoutDir | Out-Null
}
if ($stderrDir) {
    New-Item -ItemType Directory -Force -Path $stderrDir | Out-Null
}

python -m src.cli run-seeds --seed-file $SeedFile 1>> $StdoutLog 2>> $StderrLog
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

python scripts/rebuild_graph.py 1>> $StdoutLog 2>> $StderrLog
exit $LASTEXITCODE
