# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import pytest

from qtop_py import __version__
from tools.prepare_release import ROOT, release_plan


def test_release_preview_preserves_history_and_does_not_write():
    before = (ROOT / "CHANGELOG.md").read_text()
    edits = release_plan(ROOT, "0.9.20990101")
    assert (ROOT / "CHANGELOG.md").read_text() == before
    assert edits["CHANGELOG.md"].split("## " + __version__, 1)[1] == before.split("## " + __version__, 1)[1]
    assert '"2099-01-01" "qtop 0.9.20990101"' in edits["docs/qtop.1"]
    assert edits["docs/qtop-%s-GCCcore-14.3.0.eb" % __version__] is None


@pytest.mark.parametrize("version", ("0.9.20260230", __version__, "bad"))
def test_invalid_or_nonadvancing_releases_are_rejected(version):
    with pytest.raises(ValueError):
        release_plan(ROOT, version)
