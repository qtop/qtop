#!/usr/bin/env python3

"""Deterministic parser stress tests suitable for local and CI reproduction."""

import argparse
import hashlib
import random
import string
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qtop_py.plugins.pbs import PBSBatchSystem  # noqa: E402
from qtop_py.plugins.slurm import SlurmBatchSystem, SlurmStatExtractor  # noqa: E402

ALPHABET = string.ascii_letters + string.digits + "_-+*?#[](),."


def random_text(generator, max_length=80):
    """Return deterministic pseudo-random text; this is not cryptography."""
    length = generator.randint(0, max_length)  # nosec B311
    return "".join(generator.choice(ALPHABET) for _ in range(length))  # nosec B311


def exercise_slurm_states(generator, transcript):
    value = random_text(generator)
    job_state = SlurmStatExtractor._map_job_state(value)
    node_state = SlurmStatExtractor._map_node_state(value)
    assert isinstance(job_state, str) and job_state
    assert isinstance(node_state, str) and node_state
    transcript.update((job_state + "\0" + node_state).encode("utf-8"))


def exercise_slurm_nodelists(generator, transcript):
    prefix = "node%02d" % generator.randint(0, 99)  # nosec B311
    start = generator.randint(0, 90)  # nosec B311
    end = generator.randint(start, min(99, start + 8))  # nosec B311
    suffix = generator.choice(("", "-gpu", ".cluster"))  # nosec B311
    value = "%s[%02d-%02d]%s" % (prefix, start, end, suffix)
    expanded = SlurmBatchSystem.expand_nodelist(value)
    assert len(expanded) == end - start + 1
    assert all("[" not in node and "]" not in node for node in expanded)
    transcript.update("\0".join(expanded).encode("utf-8"))


def exercise_pbs_core_ranges(generator, transcript):
    start = generator.randint(0, 120)  # nosec B311
    end = generator.randint(start, min(127, start + 7))  # nosec B311
    job = "%d.server" % generator.randint(1, 999999)  # nosec B311
    parsed = list(PBSBatchSystem.get_corejob_from_range("%d-%d" % (start, end), job))
    assert parsed == [(str(core), job) for core in range(start, end + 1)]
    transcript.update(repr(parsed).encode("ascii"))


def run(seed, cases):
    """Run a bounded, reproducible corpus and return its behavior digest."""
    generator = random.Random(seed)  # nosec B311 - deterministic by design
    transcript = hashlib.sha256()
    targets = (exercise_slurm_states, exercise_slurm_nodelists, exercise_pbs_core_ranges)
    for index in range(cases):
        targets[index % len(targets)](generator, transcript)
    return transcript.hexdigest()


def main(argv=None):
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
