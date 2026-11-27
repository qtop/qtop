## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Helpers shared by several test modules: sample-backed plugin backends and tar archives."""

import io
import json
import tarfile

from qtop_py.core_api import BackendOptions


def sample_backend(cls, sample, traces, tmp_path=None, **replaced):
    """Return cls over the sample trace files (key -> name in sample), with any replaced trace written to tmp_path.

    A replacement is text, or any other value JSON-encoded.
    """
    paths = {key: str(sample / name) for key, name in traces.items()}
    for key, value in replaced.items():
        if value is not None:
            path = tmp_path / key
            path.write_text(value if isinstance(value, str) else json.dumps(value))
            paths[key] = str(path)
    return cls(paths, {}, BackendOptions())


def nodes_by_name(backend):
    """Return the backend's worker nodes keyed by domain name."""
    job_ids, _users, _states, queues = backend.get_jobs_info()
    return {node["domainname"]: node for node in backend.get_worker_nodes(job_ids, queues, None)}


def write_tar(path, members, mode="w"):
    """Write (name, text or bytes) members to a tar archive at path, in order."""
    with tarfile.open(str(path), mode) as archive:
        for name, data in members:
            data = data.encode() if isinstance(data, str) else data
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
