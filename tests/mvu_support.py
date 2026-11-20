## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Explicit MVU models for tests that drive one query or view stage directly (no qtop module globals)."""

from types import SimpleNamespace

from qtop_py.mvu import MVU_Model
from qtop_py.schedulers.registry import discover_qtop_batch_systems
from qtop_py.serialiser import GenericBatchSystem


def explicit_model(config=None, args=None, viewport=None, snapshot=None, user_to_color=None, scheduler="", display_overrides=None):
    """Return an MVU_Model built from explicit values; scheduler picks its backend by name."""
    model = MVU_Model(
        config if config is not None else {},
        args if args is not None else SimpleNamespace(COLOR="OFF"),
        viewport,
        display_overrides=display_overrides if display_overrides is not None else {},
        snapshot=snapshot,
        user_to_color=user_to_color if user_to_color is not None else {},
    )
    model.backend, model.scheduler = discover_qtop_batch_systems().get(scheduler, GenericBatchSystem), scheduler
    return model
