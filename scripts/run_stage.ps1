[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('Prepare', 'Memory', 'Storage', 'Parallel', 'Cpu', 'Preemption', 'Quota', 'Collect')]
    [string]$Stage,
    [ValidateSet('cpu', 'parallel')]
    [string]$RunName = 'cpu',
    [string]$Context = 'hw5',
    [string]$SourceEndpoint = 'http://127.0.0.1:9010',
    [string]$ClusterSourceEndpoint = 'http://host.docker.internal:9010',
    [string]$TargetEndpoint = 'http://127.0.0.1:19000',
    [switch]$PauseForScreenshots,
    [int]$TimeoutSeconds = 900
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root
$Python = Join-Path $Root '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $Python)) { throw 'Create .venv and install requirements.txt first' }
$LogRoot = Join-Path $Root 'report/logs'
$Generated = Join-Path $Root 'k8s/generated'
New-Item -ItemType Directory -Force $LogRoot,$Generated,(Join-Path $Root 'report/screenshots') | Out-Null
$Stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$Transcript = Join-Path $LogRoot "$($Stage.ToLower())-$Stamp.txt"
Start-Transcript -Path $Transcript | Out-Null

function K {
    & kubectl --context $Context @args
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed: $args" }
}
function Py {
    & $Python @args
    if ($LASTEXITCODE -ne 0) { throw "Python failed: $args" }
}
function Screenshot([string]$Name) {
    Write-Host "SCREENSHOT: report/screenshots/$Name"
    if ($PauseForScreenshots) { Read-Host 'Make the screenshot, then press Enter' | Out-Null }
}
function Storage-Memory {
    Py scripts/observe_storage.py --context $Context --output report/logs/storage-memory.jsonl
}
function Save-State([string]$Namespace, [string]$Job) {
    $Folder = Join-Path $LogRoot $Job
    New-Item -ItemType Directory -Force $Folder | Out-Null
    K -n $Namespace get job $Job -o yaml | Out-File -Encoding utf8 "$Folder/job.yaml"
    K -n $Namespace get pods -l "job-name=$Job" -o json | Out-File -Encoding utf8 "$Folder/pods.json"
    K -n $Namespace get events -o json | Out-File -Encoding utf8 "$Folder/events.json"
    $Pods = (K -n $Namespace get pods -l "job-name=$Job" -o json | ConvertFrom-Json).items
    foreach ($Pod in $Pods) {
        $Name = $Pod.metadata.name
        $Started = @($Pod.status.containerStatuses | Where-Object {
            $_.name -eq 'worker' -and ($_.state.running -or $_.state.terminated)
        })
        if ($Started.Count -eq 0) {
            'Worker container has not started; see pods.json and events.json.' |
                Out-File -Encoding utf8 "$Folder/$Name.log"
            continue
        }
        # A preempted Pod can disappear between get and logs. Record that race
        # without replacing the primary experiment result with a logging error.
        $SavedErrorPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            & kubectl --context $Context -n $Namespace logs $Name -c worker --timestamps 2>&1 |
                Out-File -Encoding utf8 "$Folder/$Name.log"
        } finally { $ErrorActionPreference = $SavedErrorPreference }
    }
}
function Wait-Job([string]$Namespace, [string]$Name) {
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    try {
        while ((Get-Date) -lt $Deadline) {
            $Job = K -n $Namespace get job $Name -o json | ConvertFrom-Json
            foreach ($Condition in $Job.status.conditions) {
                if ($Condition.status -eq 'True' -and $Condition.type -in @('Complete','Failed')) {
                    return $Condition.type
                }
            }
            Start-Sleep -Seconds 2
        }
        throw "Timeout waiting for job/$Name"
    } finally { Save-State $Namespace $Name }
}
function Apply-New([string]$Path, [string]$Namespace, [string]$Name) {
    $Existing = & kubectl --context $Context -n $Namespace get job $Name --ignore-not-found -o name
    if ($LASTEXITCODE -ne 0) { throw 'Cannot check existing Jobs' }
    if ($Existing) { throw "job/$Name already exists; archive its evidence before deleting this specific Job" }
    K apply -f $Path
}
function Success([string]$Namespace, [string]$Name) {
    $State = Wait-Job $Namespace $Name
    if ($State -ne 'Complete') { throw "job/$Name failed; see report/logs/$Name" }
}
function Check-Result([string]$Prefix, [string]$Name, [string]$Endpoint) {
    $Address = [uri]$Endpoint
    $Connection = New-Object System.Net.Sockets.TcpClient
    try {
        if (-not $Connection.ConnectAsync($Address.Host, $Address.Port).Wait(3000)) {
            throw 'Connection timeout'
        }
    } catch {
        throw "S3 endpoint $Endpoint is not reachable. Restore port-forward, then use -Stage Collect -RunName $Name for completed cpu/parallel Jobs."
    } finally { $Connection.Dispose() }
    $env:S3_ENDPOINT = $Endpoint
    $Output = "data/results_$Name"
    Py scripts/collect_results.py --prefix $Prefix --output $Output
    Py scripts/verify_results.py --reference reference/hw3/results_v2 --actual $Output --json-output "report/logs/$Name-verify.json"
    $Archive = "report/results/$Name"
    New-Item -ItemType Directory -Force $Archive | Out-Null
    Copy-Item "$Output/skills_*.csv" -Destination $Archive
    Copy-Item "$Output/summary.json" -Destination $Archive
}
function Start-Map([string]$Name, [string]$Namespace='hw5') {
    Apply-New "$Generated/$Name-partition.yaml" 'hw5' "$Name-partition"
    Success 'hw5' "$Name-partition"
    Apply-New "$Generated/$Name-map.yaml" $Namespace "$Name-map"
}
function Complete-Pipeline([string]$Name, [string]$Namespace='hw5') {
    Success $Namespace "$Name-map"
    Apply-New "$Generated/$Name-reduce.yaml" $Namespace "$Name-reduce"
    Success $Namespace "$Name-reduce"
    Check-Result "hw5/$Name/final" $Name $TargetEndpoint
    Storage-Memory
}
function Wait-Evidence([string]$Namespace, [string]$Job, [string]$Kind) {
    $Deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $Deadline) {
        $Pods = (K -n $Namespace get pods -l "job-name=$Job" -o json | ConvertFrom-Json).items
        $Events = (K -n $Namespace get events -o json | ConvertFrom-Json).items
        if ($Kind -eq 'cpu') {
            $Names = @($Pods | ForEach-Object { $_.metadata.name })
            $StartedWithError = @($Pods | Where-Object {
                $_.status.containerStatuses | Where-Object { $_.state.terminated.reason -eq 'StartError' }
            })
            if ($StartedWithError.Count) {
                Save-State $Namespace $Job
                throw 'Container StartError: inspect saved runtime message; this is not a valid CPU scheduling experiment'
            }
            $Running = @($Pods | Where-Object { $_.status.phase -eq 'Running' })
            if ($Running.Count -gt 0 -and ($Events | Where-Object { $_.involvedObject.name -in $Names -and $_.message -match 'Insufficient cpu' })) { return }
        }
        if ($Kind -eq 'running' -and ($Pods | Where-Object { $_.status.phase -eq 'Running' })) { return }
        $JobUid = (K -n $Namespace get job $Job -o json | ConvertFrom-Json).metadata.uid
        if ($Kind -eq 'quota' -and ($Events | Where-Object { $_.involvedObject.uid -eq $JobUid -and $_.reason -eq 'FailedCreate' -and $_.message -match 'exceeded quota' })) { return }
        Start-Sleep -Seconds 1
    }
    Save-State $Namespace $Job
    throw "Required $Kind evidence not observed for $Job; do not claim the experiment succeeded"
}

$OldEndpoint = $env:S3_ENDPOINT
$OldBackend = $env:S3_BACKEND
$OldFilter = $env:VARIANT_FILTER_MODE
try {
    $env:S3_BACKEND = 'boto3'
    $env:VARIANT_FILTER_MODE = 'role'
    switch ($Stage) {
        'Prepare' {
            K get nodes -o wide
            K apply -f k8s/namespace.yaml
            K apply -f k8s/config.yaml
            K apply -f k8s/priorities.yaml
            Screenshot '01-cluster.png'
        }
        'Memory' {
            $env:S3_ENDPOINT = $SourceEndpoint
            Py src/upload.py --list
            $Logs = @()
            foreach ($Index in 1..3) {
                $Name = "measure-$Index"
                Py scripts/prepare_job.py --template k8s/job-baseline.yaml --name $Name --prefix "hw5/$Name" --memory 1Gi --endpoint $ClusterSourceEndpoint --output "$Generated/$Name.yaml"
                Apply-New "$Generated/$Name.yaml" 'hw5' $Name
                Success 'hw5' $Name
                $Path = "report/logs/$Name.log"
                K -n hw5 logs "job/$Name" --timestamps | Out-File -Encoding utf8 $Path
                $Logs += $Path
            }
            Py scripts/memory_summary.py --log @Logs
            Screenshot '04-memory-baseline.png'
            Py scripts/prepare_job.py --template k8s/job-oom.yaml --name skills-oom --prefix hw5/oom --endpoint $ClusterSourceEndpoint --output "$Generated/oom.yaml"
            Apply-New "$Generated/oom.yaml" 'hw5' 'skills-oom'
            $State = Wait-Job 'hw5' 'skills-oom'
            $Pods = (K -n hw5 get pods -l job-name=skills-oom -o json | ConvertFrom-Json).items
            $Reasons = @($Pods | ForEach-Object { $_.status.containerStatuses | ForEach-Object { $_.state.terminated.reason } })
            if ($State -ne 'Failed' -or 'OOMKilled' -notin $Reasons) { throw 'Expected a real OOMKilled; adjust the OOM Job limit and name' }
            Py scripts/oom_summary.py --state-dir report/logs/skills-oom
            Screenshot '05-oomkilled.png'
            Py scripts/set_memory.py --log @Logs --template k8s/job-baseline.yaml --output k8s/job-tuned.yaml --name skills-tuned --margin 1.30
            foreach ($Index in 1..3) {
                $Name = "tuned-$Index"
                Py scripts/prepare_job.py --template k8s/job-tuned.yaml --name $Name --prefix "hw5/$Name" --endpoint $ClusterSourceEndpoint --output "$Generated/$Name.yaml"
                Apply-New "$Generated/$Name.yaml" 'hw5' $Name
                Success 'hw5' $Name
                Check-Result "hw5/$Name" $Name $SourceEndpoint
            }
            Py scripts/tuned_summary.py
            Screenshot '06-memory-tuned.png'
        }
        'Storage' {
            K apply -f k8s/s3.yaml
            K -n hw5 rollout status deployment/rustfs --timeout=300s
            Write-Host 'Open a second PowerShell: kubectl --context hw5 -n hw5 port-forward svc/rustfs 19000:9000'
            if ($PauseForScreenshots) { Read-Host 'Once port-forward is running, press Enter' | Out-Null }
            $Files = Get-Content -Raw reference/copied-files.json | ConvertFrom-Json
            $InputHash = ($Files | Where-Object { $_.destination -eq 'data/it_vacancies_2024.csv' }).sha256
            if (-not $InputHash) { throw 'Original input SHA-256 is missing' }
            Py scripts/storage.py --source-endpoint $SourceEndpoint --target-endpoint $TargetEndpoint --expected-sha256 $InputHash
            Storage-Memory
            Py scripts/storage_summary.py --context $Context
            Screenshot '03-s3.png'
        }
        'Parallel' {
            Apply-New k8s/job-partition.yaml 'hw5' skills-partition
            Success 'hw5' skills-partition
            Apply-New k8s/job-parallel.yaml 'hw5' skills-parallel
            Wait-Evidence hw5 skills-parallel running
            K -n hw5 get pods -l job-name=skills-parallel -o wide
            Screenshot '07-parallel.png'
            Success hw5 skills-parallel
            Apply-New k8s/job-reduce.yaml hw5 skills-reduce
            Success hw5 skills-reduce
            Check-Result hw5/parallel/final parallel $TargetEndpoint
            Storage-Memory
            Py scripts/results_summary.py
            Screenshot '08-results.png'
        }
        'Cpu' {
            if (-not (Test-Path "$Generated/cpu-map.yaml")) { throw 'Run prepare_scheduling.py on the current cluster first' }
            Start-Map cpu
            Wait-Evidence hw5 cpu-map cpu
            Save-State hw5 cpu-map
            Py scripts/cpu_summary.py --capture
            Screenshot '09-cpu-pending.png'
            Complete-Pipeline cpu
            Py scripts/cpu_summary.py --completed
            Screenshot '10-cpu-running.png'
        }
        'Collect' {
            # Resume only download/verification; create or delete no Kubernetes Jobs.
            if ($RunName -eq 'cpu') {
                Success hw5 cpu-map
                Success hw5 cpu-reduce
            } else {
                Success hw5 skills-parallel
                Success hw5 skills-reduce
            }
            Check-Result "hw5/$RunName/final" $RunName $TargetEndpoint
            Storage-Memory
            if ($RunName -eq 'cpu') {
                Py scripts/cpu_summary.py --completed
                Screenshot '10-cpu-running.png'
            } else {
                Py scripts/results_summary.py
                Screenshot '08-results.png'
            }
        }
        'Preemption' {
            Start-Map low
            Wait-Evidence hw5 low-map running
            Save-State hw5 low-map
            $Before = (K -n hw5 get pods -l job-name=low-map -o json | ConvertFrom-Json).items
            $RunningUids = @($Before | Where-Object { $_.status.phase -eq 'Running' } | ForEach-Object { $_.metadata.uid })
            Apply-New "$Generated/urgent.yaml" hw5 urgent
            # Save each victim's state/log before Job controller removes it.
            $Victims = @{}
            $Deadline = (Get-Date).AddSeconds(180)
            while ((Get-Date) -lt $Deadline -and $Victims.Count -eq 0) {
                $Pods = (K -n hw5 get pods -l job-name=low-map -o json | ConvertFrom-Json).items
                foreach ($Pod in $Pods) {
                    $Disrupted = $Pod.status.conditions | Where-Object { $_.type -eq 'DisruptionTarget' -and $_.reason -eq 'PreemptionByScheduler' }
                    if ($Disrupted) {
                        $Victims[$Pod.metadata.name] = $Pod
                        $Pod | ConvertTo-Json -Depth 40 | Out-File -Encoding utf8 "$LogRoot/victim-$($Pod.metadata.name).json"
                        Save-State hw5 low-map
                    }
                }
                # Events survive a short-lived victim Pod and are matched by UID, not reused names.
                $Events = (K -n hw5 get events -o json | ConvertFrom-Json).items
                foreach ($Event in $Events) {
                    if ($Event.reason -eq 'Preempted' -and $Event.involvedObject.uid -in $RunningUids) {
                        $Victims[$Event.involvedObject.name] = $Event
                        $Event | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 "$LogRoot/preempted-$($Event.involvedObject.name).json"
                    }
                }
                Start-Sleep -Milliseconds 500
            }
            if ($Victims.Count -eq 0) { Save-State hw5 low-map; throw 'No preempted victim was captured; inspect events and repeat with a new run' }
            Py scripts/preemption_summary.py --context $Context --capture
            Screenshot '11-preemption.png'
            Success hw5 urgent
            Check-Result hw5/urgent urgent $TargetEndpoint
            Complete-Pipeline low
        }
        'Quota' {
            K apply -f k8s/quota.yaml
            K apply -f "$Generated/quota-config.yaml"
            Start-Map quota hw5-quota
            Wait-Evidence hw5-quota quota-map quota
            Save-State hw5-quota quota-map
            K -n hw5-quota describe resourcequota hw5-quota
            K -n hw5-quota get events --sort-by=.metadata.creationTimestamp
            Screenshot '12-quota.png'
            Complete-Pipeline quota hw5-quota
            Py scripts/quota_summary.py --context $Context --capture
            Screenshot '13-quota-released.png'
        }
    }
} finally {
    $env:S3_ENDPOINT = $OldEndpoint
    $env:S3_BACKEND = $OldBackend
    $env:VARIANT_FILTER_MODE = $OldFilter
    Stop-Transcript | Out-Null
}
