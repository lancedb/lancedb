# Builds the Windows artifacts (node binaries).
# Usage:  .\ci\build_windows_artifacts.ps1 [target]
# Targets supported:
# - x86_64-pc-windows-msvc
# - i686-pc-windows-msvc

function Prebuild-Rust {
    param (
        [string]$target
    )

    # Building here for the sake of easier debugging.
    Push-Location -Path "rust/ffi/node"
    Write-Host "Building rust library for $target"
    $env:RUST_BACKTRACE=1
    cargo build --release --target $target
    Pop-Location
}

function Build-NodeBinaries {
    param (
        [string]$target
    )

    Push-Location -Path "node"
    Write-Host "Building node library for $target"
    npm run build-release -- --target $target
    npm run pack-build -- --target $target
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
