$ErrorActionPreference = "Stop"

$envPath = ".\.env"
if (!(Test-Path $envPath)) { throw "Missing .env file." }
Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*#') { return }
    if ($_ -match '^\s*$') { return }
    $parts = $_.Split('=',2)
    if ($parts.Count -eq 2) {
        [System.Environment]::SetEnvironmentVariable($parts[0], $parts[1])
    }
}

$GIT_USERNAME = $env:GIT_USERNAME
$GIT_PAT      = $env:GIT_PAT
$REMOTE       = $env:REMOTE_REPO_URL
$PARENT_DIR   = $env:PARENT_REPO_DIR
$TARGET_NAME  = $env:TARGET_DIR_NAME

if (!$GIT_USERNAME -or !$GIT_PAT -or !$REMOTE -or !$PARENT_DIR -or !$TARGET_NAME) {
    throw "Missing one or more required env vars."
}

Push-Location $PARENT_DIR
git -c credential.helper= `
    -c f"credential.helper=!f() { echo username=$GIT_USERNAME; echo password=$GIT_PAT; }; f" `
    clone $REMOTE $TARGET_NAME

Write-Host "Cloned into: " (Resolve-Path $TARGET_NAME)

Push-Location $TARGET_NAME
git remote set-url origin $REMOTE
Pop-Location; Pop-Location