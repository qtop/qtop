# SPDX-License-Identifier: MIT
"""Coverage-guided inputs for the existing deterministic parser checks."""

import hashlib
import logging
import sys

import atheris

with atheris.instrument_imports():
    from tools.deterministic_fuzz import exercise_pbs_core_ranges, exercise_slurm_nodelists, exercise_slurm_states


class Inputs:
    """Adapt Atheris bytes to the bounded randint/choice interface of our checks."""

    def __init__(self, provider):
        self.provider = provider

    def randint(self, lower, upper):
        return self.provider.ConsumeIntInRange(lower, upper)

    def choice(self, values):
        return values[self.randint(0, len(values) - 1)]


@atheris.instrument_func
def fuzz_one_input(data):
    provider = atheris.FuzzedDataProvider(data)
    targets = (exercise_slurm_states, exercise_slurm_nodelists, exercise_pbs_core_ranges)
    target = targets[provider.ConsumeIntInRange(0, len(targets) - 1)]
    target(Inputs(provider), hashlib.sha256())


if __name__ == "__main__":
    # Malformed host lists deliberately generate warnings in the shared checks.
    logging.disable(logging.WARNING)
    atheris.Setup(sys.argv, fuzz_one_input)
    atheris.Fuzz()
