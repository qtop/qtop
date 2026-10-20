# SPDX-License-Identifier: MIT
import os
import subprocess
import sys

import pytest

pytest.importorskip("atheris")

from tools import atheris_fuzz, deterministic_fuzz


@pytest.mark.parametrize("data", (b"", b"\x01", b"\x02", bytes(range(256)), b"\xff" * 256))
def test_shared_parser_checks_accept_fuzzer_inputs(data):
    atheris_fuzz.fuzz_one_input(data)


def test_shared_assertions_are_not_suppressed(monkeypatch):
    monkeypatch.setattr(deterministic_fuzz.SlurmStatExtractor, "_map_job_state", lambda state: "")
    with pytest.raises(AssertionError):
        atheris_fuzz.fuzz_one_input(b"")


def test_bounded_fuzz_run_collects_coverage(tmp_path):
    command = [sys.executable, "-m", "tools.atheris_fuzz", "-runs=100", "-seed=488", "-max_len=256", "-artifact_prefix=" + str(tmp_path) + os.sep]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "cov:" in result.stderr and "DONE" in result.stderr
