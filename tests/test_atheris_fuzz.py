## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import importlib
import os
import subprocess
import sys

import pytest

from tools import deterministic_fuzz

pytest.importorskip("atheris")
atheris_fuzz = importlib.import_module("tools.atheris_fuzz")


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


@pytest.mark.parametrize("selector", range(3, 9))
def test_document_targets_do_not_suppress_unexpected_failures(monkeypatch, selector):
    def broken(*args):
        raise RuntimeError("unexpected parser failure")

    monkeypatch.setattr(atheris_fuzz, "exercise_document", broken)
    with pytest.raises(RuntimeError, match="unexpected parser failure"):
        atheris_fuzz.fuzz_one_input(bytes([selector]) + b"payload")


def test_seed_corpus_preserves_discovered_and_modified_inputs(tmp_path):
    (tmp_path / "discovered").write_bytes(b"crash regression")
    (tmp_path / "yaml").write_bytes(b"modified seed")
    atheris_fuzz.seed_corpus(tmp_path)
    assert (tmp_path / "discovered").read_bytes() == b"crash regression"
    assert (tmp_path / "yaml").read_bytes() == b"modified seed"
    assert (tmp_path / "pbs").read_bytes() == b'\x05{"Jobs":{}}'


def test_atheris_replays_an_existing_input_without_seeding_it(tmp_path):
    sample = tmp_path / "replay"
    sample.write_bytes(b"\x05{}")
    result = subprocess.run(
        [sys.executable, "-m", "tools.atheris_fuzz", str(sample)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "Running:" in result.stderr
    assert sample.read_bytes() == b"\x05{}"
    assert list(tmp_path.iterdir()) == [sample]
