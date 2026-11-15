## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Coverage-guided inputs for the existing deterministic parser checks."""

import hashlib
import logging
import sys
from pathlib import Path

import atheris

from tools.private_output import private_output_dir

with atheris.instrument_imports():
    from tools.deterministic_fuzz import DOCUMENT_TARGETS, exercise_document, exercise_pbs_core_ranges, exercise_slurm_nodelists, exercise_slurm_states


class Inputs:
    """Adapt Atheris bytes to the bounded randint/choice interface of our checks."""

    def __init__(self, provider):
        self.provider = provider

    def randint(self, lower, upper):
        """Return an integer in [lower, upper] drawn from the fuzzer's bytes."""
        return self.provider.ConsumeIntInRange(lower, upper)

    def choice(self, values):
        """Return one of values, chosen by the fuzzer's bytes."""
        return values[self.randint(0, len(values) - 1)]


@atheris.instrument_func
def fuzz_one_input(data):
    """Run one fuzzer-chosen parser check on data."""
    selector = data[0] % (3 + len(DOCUMENT_TARGETS)) if data else 0
    if selector >= 3:
        exercise_document(DOCUMENT_TARGETS[selector - 3], data[1:])
        return
    provider = atheris.FuzzedDataProvider(data[1:])
    targets = (exercise_slurm_states, exercise_slurm_nodelists, exercise_pbs_core_ranges)
    target = targets[selector]
    target(Inputs(provider), hashlib.sha256())


def seed_corpus(directory):
    """Add reviewed seeds without replacing inputs discovered by previous runs."""
    output = private_output_dir(directory)
    for seed in sorted((Path(__file__).resolve().parents[1] / "tests" / "fuzz_corpus").glob("*.hex")):
        target = output / seed.stem
        if not target.exists():
            target.write_bytes(bytes.fromhex(seed.read_text()))


if __name__ == "__main__":
    # Malformed host lists deliberately generate warnings in the shared checks.
    logging.disable(logging.WARNING)
    if "--seed-corpus" in sys.argv:
        sys.argv.remove("--seed-corpus")
        seed_corpus(sys.argv[-1])
    atheris.Setup(sys.argv, fuzz_one_input)
    atheris.Fuzz()
