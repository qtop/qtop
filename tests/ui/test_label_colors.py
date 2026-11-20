## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from qtop_py.ui.mvu_view import TextDisplay, label_group_colors

ROOT = Path(__file__).resolve().parents[2]


def test_issue_197_example_groups_by_name_then_in_batches_of_five():
    hosts = ["computenode%02d" % nr for nr in range(1, 5)] + ["transfernode%02d" % nr for nr in range(1, 11)]
    assert label_group_colors(hosts, ["W", "B"]) == ["W"] * 4 + ["B"] * 5 + ["W"] * 5


@pytest.mark.parametrize(
    ("hosts", "expected"),
    (
        ([], []),
        (["wn1.site.org", "wn2.site.org", "wn10.site.org", "gpu1.site.org"], ["W", "W", "W", "B"]),  # digits anywhere, FQDNs
        (["a1", "b1", "a2"], ["W", "B", "W"]),  # interleaved groups still alternate
        (["n%d" % nr for nr in range(12)], ["W"] * 5 + ["B"] * 5 + ["W"] * 2),
    ),
    ids=("empty", "fqdn", "interleaved", "long-group"),
)
def test_label_group_colors(hosts, expected):
    assert label_group_colors(hosts, ["W", "B"]) == expected


def test_label_group_colors_honours_group_size_and_palette():
    assert label_group_colors(["n%d" % nr for nr in range(6)], ["X", "Y", "Z"], group_size=2) == ["X", "X", "Y", "Y", "Z", "Z"]


def test_label_colours_get_separators_where_label_characters_do():
    labels = TextDisplay._insert_separators(None, "abcdef", "|", 2)
    colours = TextDisplay._insert_separators(None, ["W", "W", "B", "B", "W", "W"], ["S"], 2)
    assert len(colours) == len(labels)
    assert [colour for label, colour in zip(labels, colours) if label == "|"] == ["S"] * labels.count("|")


def test_named_label_lines_change_colour_every_five_nodes(tmp_path):
    env = dict(os.environ, HOME=str(tmp_path), PYTHONPATH=str(ROOT))
    env.pop("QTOP_SCHEDULER", None)
    sample = ROOT / "tests/plugins/slurm_samples/large_cluster"
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", "slurm", "-s", str(sample), "-F", "-c", "ON"]
    command += ["-o", "transpose_wn_matrices=False", "-o", "savepath=%s" % (tmp_path / "output")]
    result = subprocess.run(command, cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    assert result.returncode == 0, result.stdout
    label_line = next(line for line in result.stdout.splitlines() if line.endswith("={___ID___}"))
    colors = re.findall(r"\x1b\[([0-9;]+)m[^\x1b=]", label_line)
    assert colors[:15] == ["1;37"] * 5 + ["1;34"] * 5 + ["1;37"] * 5
