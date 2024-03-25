# Builds the Windows artifacts (nodejs binaries).
# Usage:  .\ci\build_windows_artifacts_nodejs.ps1 [target]
# Targets supported:
# - x86_64-pc-windows-msvc
# - i686-pc-windows-msvc

function Prebuild-Rust {
    param (
        [string]$target
    )

    # Building here for the sake of easier debugging.
    Push-Location -Path "rust/lancedb"
    Write-Host "Building rust library for $target"
    $env:RUST_BACKTRACE=1
    cargo build --release --target $target
    Pop-Location
}

function Build-NodeBinaries {
    param (
        [string]$target
    )

    Push-Location -Path "nodejs"
    Write-Host "Building nodejs library for $target"
    $env:RUST_TARGET=$target
    npm run build-release
    Pop-Location
}

$targets = $args[0]
if (-not $targets) {
    $targets = "x86_64-pc-windows-msvc"
}

Write-Host "Building artifacts for targets: $targets"
foreach ($target in $targets) {
    Prebuild-Rust $target
    Build-NodeBinaries $target
}
