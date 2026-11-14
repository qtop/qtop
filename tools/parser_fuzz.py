# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Bound direct document inputs for parser and portable-snapshot fuzzing."""

import io
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import ParseError

from qtop_py import configuration, sample
from qtop_py.plugins.oar import OarStatExtractor
from qtop_py.plugins.pbs import PBSStatExtractor
from qtop_py.plugins.sge import SGEStatExtractor
from qtop_py.plugins.slurm import SlurmStatExtractor

DOCUMENT_TARGETS = ("yaml", "sge", "pbs", "oar", "slurm", "snapshot")


def exercise_document(target: str, data: bytes) -> None:
    """Accept bounded documents or the parsers' known malformed-input rejections."""
    # Syntax and shape failures are expected; assertions, resource failures,
    # OS errors and unrelated exceptions must remain visible to the fuzzer.
    rejected = (ValueError, TypeError, KeyError, IndexError, StopIteration, ParseError)
    options = SimpleNamespace(ANONYMIZE=False)
    with tempfile.TemporaryDirectory(prefix="qtop-fuzz-") as directory:
        path = Path(directory) / "input"
        path.write_bytes(data[:4096])
        try:
            if target == "yaml":
                configuration.parse_layer(str(path))
            elif target == "sge":
                SGEStatExtractor({}, options, {}).get_xml_tree(str(path))
            elif target == "pbs":
                PBSStatExtractor({}, options)._extract_qstat_json(str(path))
            elif target == "oar":
                OarStatExtractor({}, options).extract_qstat(str(path))
            elif target == "slurm":
                SlurmStatExtractor({}, options).extract_squeue(str(path))
            elif target == "snapshot":
                archive_path = Path(directory) / "sample.tar"
                with tarfile.open(str(archive_path), "w") as archive:
                    member = tarfile.TarInfo(sample.SNAPSHOT_NAME)
                    member.size = min(len(data), 4096)
                    archive.addfile(member, io.BytesIO(data[:4096]))
                sample.load_snapshot(str(archive_path))
            else:
                raise AssertionError("unknown document target")
        except rejected:
            return
