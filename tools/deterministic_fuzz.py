#!/usr/bin/env python3
## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT

"""Deterministic parser stress tests suitable for local and CI reproduction."""

import argparse
import hashlib
import io
import random
import string
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import ParseError

from qtop_py import configuration, sample
from qtop_py.plugins.oar import OarStatExtractor
from qtop_py.plugins.pbs import PBSBatchSystem, PBSStatExtractor
from qtop_py.plugins.sge import SGEStatExtractor
from qtop_py.plugins.slurm import SlurmBatchSystem, SlurmStatExtractor

ALPHABET = string.ascii_letters + string.digits + "_-+*?#[](),."


def random_text(generator, max_length=80):
    """Return deterministic pseudo-random text; this is not cryptography."""
    length = generator.randint(0, max_length)  # nosec B311
    return "".join(generator.choice(ALPHABET) for _ in range(length))  # nosec B311


def exercise_slurm_states(generator, transcript):
    """Check that random text maps to a non-empty Slurm job and node state."""
    value = random_text(generator)
    job_state = SlurmStatExtractor._map_job_state(value)
    node_state = SlurmStatExtractor._map_node_state(value)
    assert isinstance(job_state, str) and job_state
    assert isinstance(node_state, str) and node_state
    transcript.update((job_state + "\0" + node_state).encode("utf-8"))


def exercise_slurm_nodelists(generator, transcript):
    """Check that a random node range expands to the expected host names."""
    prefix = "node%02d" % generator.randint(0, 99)  # nosec B311
    start = generator.randint(0, 90)  # nosec B311
    end = generator.randint(start, min(99, start + 8))  # nosec B311
    suffix = generator.choice(("", "-gpu", ".cluster"))  # nosec B311
    value = "%s[%02d-%02d]%s" % (prefix, start, end, suffix)
    expanded = SlurmBatchSystem.expand_nodelist(value)
    assert len(expanded) == end - start + 1
    assert all("[" not in node and "]" not in node for node in expanded)
    transcript.update("\0".join(expanded).encode("utf-8"))
    grouped = SlurmBatchSystem.expand_nodelist(value + ",login")
    assert grouped == expanded + ["login"]
    malformed = generator.choice(("node[", "node[]", "node[2-1]", "node[x-y]", "node01,,node02"))  # nosec B311
    assert SlurmBatchSystem.expand_nodelist(malformed) == []
    transcript.update("\0".join(grouped).encode("utf-8"))


def exercise_pbs_core_ranges(generator, transcript):
    """Check that a random PBS core range yields one (core, job) pair per core."""
    start = generator.randint(0, 120)  # nosec B311
    end = generator.randint(start, min(127, start + 7))  # nosec B311
    job = "%d.server" % generator.randint(1, 999999)  # nosec B311
    parsed = list(PBSBatchSystem.get_corejob_from_range("%d-%d" % (start, end), job))
    assert parsed == [(str(core), job) for core in range(start, end + 1)]
    transcript.update(repr(parsed).encode("ascii"))


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


def run(seed, cases):
    """Run a bounded, reproducible corpus and return its behavior digest."""
    generator = random.Random(seed)  # nosec B311 - deterministic by design
    transcript = hashlib.sha256()
    targets = (exercise_slurm_states, exercise_slurm_nodelists, exercise_pbs_core_ranges)
    for index in range(cases):
        targets[index % len(targets)](generator, transcript)
    return transcript.hexdigest()


def main(argv=None):
    """Run the seeded cases and print their transcript digest."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=488, help="reproduction seed (default: 488)")
    parser.add_argument("--cases", type=int, default=5000, help="bounded number of cases (default: 5000)")
    args = parser.parse_args(argv)
    if args.cases < 1:
        parser.error("--cases must be positive")

    digest = run(args.seed, args.cases)
    print("deterministic fuzz: %d cases, seed %d, sha256 %s" % (args.cases, args.seed, digest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
