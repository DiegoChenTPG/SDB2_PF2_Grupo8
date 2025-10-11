param(
    [int]$StepDelaySeconds = 20,
    [string]$ApiUrl = "http://localhost:8000",
    [string]$Stanza = "bases2-db",
    [string]$DbName = "bases2_proyectos",
    [string]$Node = "pg-bases2"   # se deja como fijo ya que no se haran backups en medio de failover y failback
)

$ErrorActionPreference = "Stop"

function Convert-ToBytes([string]$s) {
    if (-not $s) { return $null }
    if ($s -match '^\s*([\d\.]+)\s*([KMGTP]?B)\s*$') {
        $n = [double]$Matches[1]; $u = $Matches[2].ToUpper()
        switch ($u) {
        "B" { [long]$n } "KB" { [long]($n*1KB) } "MB" { [long]($n*1MB) }
        "GB" { [long]($n*1GB) } "TB" { [long]($n*1TB) } default { $null }
        }
    }
}

function Run-Backup([string]$Type) {
    if ($Type -notin @("full","diff","incr")) { throw "Tipo invalido: $Type" }
    Write-Host ">> $Type en $Node (stanza=$Stanza)..." -ForegroundColor Cyan

    $cmd = @("bash","-lc","pgbackrest --stanza=$Stanza --type=$Type --log-level-console=info --start-fast backup")
    $out = docker exec -u postgres $Node @cmd 2>&1; $code = $LASTEXITCODE
    if ($code -ne 0) { Write-Host $out; throw "Fallo backup $Type ($code)" }

    $jsonInfo = docker exec -u postgres $Node pgbackrest --stanza=$Stanza --output=json info
    $info = $jsonInfo | ConvertFrom-Json

    # Manejar raiz como array u objeto
    $root = $info
    if ($info -is [System.Array]) { $root = $info[0] }

    # Obtener lista de backups (con fallback)
    $bkList = $root.backup[0].backup
    if (-not $bkList) { $bkList = $root.backup.backup }
    if (-not $bkList) { throw "No se encontraron backups en el JSON." }

    $bk = $bkList[-1]



    # --- obtener campos de forma segura ---
    # label
    $label = $bk.label

    # when (ISO, UTC): usa timestamp.stop si existe, si no, usa ahora
    $tsStopRaw = $null
    if ($bk.PSObject.Properties.Name -contains 'timestamp' -and $bk.timestamp) {
        if ($bk.timestamp.PSObject.Properties.Name -contains 'stop' -and $bk.timestamp.stop) {
            $tsStopRaw = $bk.timestamp.stop
        } elseif ($bk.timestamp.PSObject.Properties.Name -contains 'stop-epoch' -and $bk.timestamp.'stop-epoch') {
            # algunas versiones incluyen stop-epoch
            $tsStopRaw = [DateTimeOffset]::FromUnixTimeSeconds([int64]$bk.timestamp.'stop-epoch').UtcDateTime
        }
    }
    if ($tsStopRaw) {
        $when = (Get-Date $tsStopRaw).ToUniversalTime().ToString("o")
    } else {
        $when = (Get-Date).ToUniversalTime().ToString("o")
    }

    # tamaños
    $dbBytes = Convert-ToBytes $bk."database-backup-size"

    $repoSetSize = $null
    if ($bk.repo1 -and ($bk.repo1.PSObject.Properties.Name -contains 'backup-set-size')) {
        $repoSetSize = $bk.repo1."backup-set-size"
    }
    $rpBytes = Convert-ToBytes $repoSetSize

    # WAL (puede no venir en sets muy pequeños)
    $walS = $null; $walE = $null
    if ($bk.PSObject.Properties.Name -contains 'wal' -and $bk.wal) {
        if ($bk.wal.PSObject.Properties.Name -contains 'start') { $walS = $bk.wal.start }
        if ($bk.wal.PSObject.Properties.Name -contains 'stop')  { $walE = $bk.wal.stop }
    }


    $payload = @{
        when = $when; node = $Node; stanza = $Stanza; dbname = $DbName; type = $Type
        label = $label; repo_size_bytes = $rpBytes; db_backup_bytes = $dbBytes
        duration_sec = $null; wal_start = $walS; wal_stop = $walE; notes = "sim"
    }

    try {
        $resp = Invoke-RestMethod -Uri "$ApiUrl/backup/log" -Method Post `
        -Headers @{ "Content-Type" = "application/json" } `
        -Body ($payload | ConvertTo-Json -Depth 5)
        Write-Host "Log Redis id=$($resp.id) label=$label type=$Type" -ForegroundColor Green
    } catch { Write-Warning "No se pudo registrar en Redis: $($_.Exception.Message)" }
}

function Pause-Step([string]$cap) {
    if ($StepDelaySeconds -gt 0) { Write-Host "Espera $StepDelaySeconds s ($cap)"; Start-Sleep -Seconds $StepDelaySeconds }
}

Write-Host "=== Simulacion Backups (primario fijo=$Node) ===" -ForegroundColor Yellow

# Dia 1: Completo
Run-Backup full; Pause-Step "fin Dia 1"

# Dia 2: Incremental
Run-Backup incr; Pause-Step "fin Dia 2"

# Dia 3: Incremental + Diferencial
Run-Backup incr; Run-Backup diff; Pause-Step "fin Dia 3"

# Dia 4: Incremental
Run-Backup incr; Pause-Step "fin Dia 4"

# Dia 5: Incremental + Diferencial
Run-Backup incr; Run-Backup diff; Pause-Step "fin Dia 5"

# Dia 6: Diferencial + Completo
Run-Backup diff; Run-Backup full

Write-Host "=== Simulacion completada ===" -ForegroundColor Yellow
