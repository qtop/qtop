#!/usr/bin/env bash
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Vadik Malik
##
## SPDX-License-Identifier: MIT
##
# Shared nightly-matrix lane runner, used identically by GitHub Actions
# (.github/workflows/nightly-matrix.yml) and GitLab CI
# (ci/nightly-matrix.gitlab-ci.yml) so the >30 distro x python lanes stay
# DRY across forges (#488). The lane purpose is interpreter/runtime
# compatibility -- diff-health checks stay in the merge-request pipelines,
# which is why lanes call git-free Makefile targets only:
#
#   ci          -> make nightly-ci   (pinned CI deps, pytest, sample gates)
#   compat-py36 -> make compat-py36  (dependency-light stdlib-only gate)
#
# Usage: matrix_lane.sh FAMILY PKGS PYBIN TARGET
#   FAMILY  pythonimg|pypyimg|debian|rhel|fedora|suse|arch|amazon
#   PKGS    comma-separated distro packages to install, or "-" for none
#   PYBIN   interpreter to hand to make as PYTHON=...
#   TARGET  ci|compat-py36

set -euo pipefail

FAMILY="${1:?FAMILY required}"
PKGS="${2:?PKGS required (use - for none)}"
PYBIN="${3:?PYBIN required}"
TARGET="${4:?TARGET required (ci|compat-py36)}"

log() { printf '\n=== %s ===\n' "$*"; }

PKG_LIST=""
if [ "${PKGS}" != "-" ]; then
    PKG_LIST="$(printf '%s' "${PKGS}" | tr ',' ' ')"
fi

fix_eol_apt_sources() {
    # Debian buster and Ubuntu focal live on archive mirrors after EOL; keep
    # those lanes runnable so the matrix can still exercise old interpreters.
    if grep -qs 'buster' /etc/os-release && [ -f /etc/apt/sources.list ]; then
        sed -i \
            -e 's|deb.debian.org/debian-security|archive.debian.org/debian-security|g' \
            -e 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' \
            -e 's|deb.debian.org/debian|archive.debian.org/debian|g' \
            -e '/buster-updates/d' /etc/apt/sources.list || true
        APT_OPTS="-o Acquire::Check-Valid-Until=false"
    fi
    if grep -qs 'focal' /etc/os-release && ! apt-get update >/dev/null 2>&1; then
        sed -i \
            -e 's|archive.ubuntu.com/ubuntu|old-releases.ubuntu.com/ubuntu|g' \
            -e 's|security.ubuntu.com/ubuntu|old-releases.ubuntu.com/ubuntu|g' \
            /etc/apt/sources.list || true
    fi
}

APT_OPTS=""
log "lane setup: family=${FAMILY} pkgs=${PKGS} python=${PYBIN} target=${TARGET}"
case "${FAMILY}" in
    pythonimg|pypyimg|debian)
        fix_eol_apt_sources
        apt-get update ${APT_OPTS}
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            make ca-certificates ${PKG_LIST}
        ;;
    rhel|fedora|amazon)
        # No "which" package needed: qtop autodetection now uses stdlib
        # shutil.which() (qtop_py/qtop.py), so the external binary the first
        # live run tripped over on minimal images is no longer a dependency.
        dnf -y install make findutils ${PKG_LIST}
        ;;
    suse)
        # refresh is best-effort: openSUSE mirrors intermittently return
        # partial-refresh errors (exit 4); install fails on its own merit.
        zypper --non-interactive refresh || true
        zypper --non-interactive install make ${PKG_LIST}
        ;;
    arch)
        pacman -Sy --noconfirm make ${PKG_LIST}
        ;;
    *)
        echo "unknown family: ${FAMILY}" >&2
        exit 2
        ;;
esac

log "interpreter"
command -v "${PYBIN}"
"${PYBIN}" --version

case "${TARGET}" in
    ci)
        # Official python/pypy images ship a writable pip; distro interpreters
        # get an isolated venv so externally-managed-environment policies
        # (PEP 668: Debian, Fedora, Arch) and root site-packages stay untouched.
        if [ "${FAMILY}" = "pythonimg" ] || [ "${FAMILY}" = "pypyimg" ]; then
            LANE_PY="${PYBIN}"
        else
            log "creating lane venv"
            "${PYBIN}" -m venv /tmp/qtop-lane-venv
            LANE_PY=/tmp/qtop-lane-venv/bin/python
        fi
        log "make ci-deps"
        make ci-deps PYTHON="${LANE_PY}"
        log "make nightly-ci"
        make nightly-ci PYTHON="${LANE_PY}"
        ;;
    compat-py36)
        log "make compat-py36"
        make compat-py36 PYTHON="${PYBIN}"
        ;;
    *)
        echo "unknown target: ${TARGET}" >&2
        exit 2
        ;;
esac

log "lane done: ${FAMILY} ${PYBIN} ${TARGET}"
