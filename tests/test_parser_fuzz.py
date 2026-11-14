# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import pytest

from tools.parser_fuzz import DOCUMENT_TARGETS, exercise_document


@pytest.mark.parametrize("target", DOCUMENT_TARGETS)
@pytest.mark.parametrize("data", (b"", b"invalid\x00\xff", b"{}", b"[]", b"null"))
def test_document_parsers_accept_or_reject_bounded_inputs(target, data):
    exercise_document(target, data)


@pytest.mark.parametrize(
    "target,data",
    (
        ("yaml", b"scheduler: demo\n"),
        ("sge", b"<job_info/>"),
        ("pbs", b'{"Jobs":{}}'),
        ("oar", b"header\n-----\n"),
        ("slurm", b"1|alice|R|debug|1|node1\n"),
        ("snapshot", b'{"format_version":1,"scheduler":"demo","state":{"jobs":[],"nodes":[],"queues":[]},"config":{},"options":{}}'),
    ),
)
def test_document_targets_include_valid_seed_inputs(target, data):
    exercise_document(target, data)
