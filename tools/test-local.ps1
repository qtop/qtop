# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
# Run the Linux suite through WSL; preserve native argument boundaries.
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$LinuxRoot = & wsl.exe --exec wslpath -a $RepoRoot
if ($LASTEXITCODE -ne 0) { throw 'Cannot access this checkout from WSL.' }
& wsl.exe --cd $LinuxRoot --exec python3 -m tools.local_tests @args
exit $LASTEXITCODE
