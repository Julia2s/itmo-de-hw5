param()

$ErrorActionPreference = 'Stop'
$reportDirectory = Join-Path (Split-Path $PSScriptRoot -Parent) 'report'
$engine = Get-Command pdflatex -ErrorAction SilentlyContinue
if (-not $engine) {
    throw 'pdfLaTeX was not found on PATH. Install TeX Live or MiKTeX, or use Overleaf with the pdfLaTeX compiler.'
}

Push-Location -LiteralPath $reportDirectory
try {
    New-Item -ItemType Directory -Path 'build' -Force | Out-Null
    foreach ($pass in 1..2) {
        Write-Host "pdfLaTeX pass $pass/2"
        & $engine.Source '-interaction=nonstopmode' '-halt-on-error' '-file-line-error' '-output-directory=build' 'report.tex'
        if ($LASTEXITCODE -ne 0) {
            throw "pdfLaTeX failed on pass $pass. See report/build/report.log."
        }
    }
    Copy-Item -LiteralPath 'build/report.pdf' -Destination 'report.pdf' -Force
    Write-Host "PDF: $(Join-Path $reportDirectory 'report.pdf')"
}
finally {
    Pop-Location
}
