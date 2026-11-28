## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Warn once per run.

Backends and file fallbacks are rebuilt on every collection, so watch mode would repeat the same
warning each refresh; they ask first_time() instead of keeping a set of their own.
"""

from typing import Hashable, Set, Tuple

_seen: Set[Tuple[Hashable, ...]] = set()


def first_time(*key: Hashable) -> bool:
    """Return True the first time this key is seen in the process, then False."""
    if key in _seen:
        return False
    _seen.add(key)
    return True


def reset() -> None:
    """Forget every key, as if a new run started (tests use this between cases)."""
    _seen.clear()
