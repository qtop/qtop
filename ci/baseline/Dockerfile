## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
##
## BASELINE: AlmaLinux 8 with the Python 3.6 lane's packages preinstalled, so CI
## jobs run no dnf. Built and published by .github/workflows/baseline-image.yml;
## Dependabot bumps the digest below.
FROM almalinux:10@sha256:5a4b41a427c9717be8f4ac2a32aa22efbae5f9124fbcb79a6765394f312acfe8

# Primary repo.almalinux.org instead of the mirror list, no Extras, and dnf drops a source slower than 50 kB/s after 10 s.
RUN sed -i -e 's|^mirrorlist=|#mirrorlist=|' -e 's|^# *baseurl=|baseurl=|' /etc/yum.repos.d/almalinux*.repo \
 && dnf -y --disablerepo='extras*' --setopt=minrate=50k --setopt=timeout=10 --setopt=install_weak_deps=False --nodocs install make findutils python36 \
 && dnf clean all \
 && rm -rf /var/cache/dnf \
 && python3.6 --version
