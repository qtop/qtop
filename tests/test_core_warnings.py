## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Once-per-run warnings."""

from qtop_py import core_warnings


def test_first_time_is_true_once_per_key_until_reset():
    assert core_warnings.first_time("a", 1)
    assert not core_warnings.first_time("a", 1)
    assert core_warnings.first_time("a", 2)
    core_warnings.reset()
    assert core_warnings.first_time("a", 1)
