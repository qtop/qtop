## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The legacy qtop adapters keep working for one more release, but warn."""

import pytest

from qtop_py import qtop
from qtop_py.ui.viewport import Viewport

DEPRECATED = r"is deprecated since 0\.9\.20261110"


class _Base(object):
    def __init__(self, *args, **kwargs):
        pass


class _Adapter(qtop.MVU_LegacyAdapter, _Base):
    pass


@pytest.mark.parametrize(
    "call",
    (
        qtop.MVU_legacy_model,
        lambda: qtop.colorize("x", color_func="Red_L"),
        _Adapter,
    ),
    ids=("MVU_legacy_model", "colorize", "adapter classes"),
)
def test_legacy_entry_points_warn(call):
    with pytest.warns(DeprecationWarning, match=DEPRECATED):
        call()


def test_control_qtop_warns_and_still_works(monkeypatch):
    monkeypatch.setattr(qtop, "dynamic_config", {}, raising=False)
    view = Viewport(20, 30)
    view.set_term_size(10, 20)
    view.max_height, view.max_width = 100, 200
    with pytest.warns(DeprecationWarning, match=DEPRECATED):
        qtop.control_qtop(view, "j", None, [0, 0, 0, 0], [0, 0, 0, 0])
    assert view.v_start == 29  # one 9-line page of a 10-row terminal
