from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


SAMPLE_SET = Path(__file__).with_name("pbs_sample_set.txt")
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parents[3] / "qtop-test-repo" / "qtop5" / "results"
RESULTS_DIR = Path(os.environ.get("QTOP_PBS_RESULTS_DIR", DEFAULT_RESULTS_DIR))
QTOP_ROOT = Path(__file__).resolve().parents[2]


def _load_samples() -> list[str]:
    return [line.strip() for line in SAMPLE_SET.read_text().splitlines() if line.strip()]


def _run_sample(sample_name: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "qtop_py.cli",
            "-b",
            "pbs",
            "-s",
            str(RESULTS_DIR / sample_name),
            "-c",
            "ON",
        ],
        cwd=QTOP_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )


SAMPLES = _load_samples()


@pytest.mark.skipif(not RESULTS_DIR.exists(), reason="PBS sample corpus is not available locally")
@pytest.mark.parametrize("sample_name", SAMPLES)
def test_pbs_corpus_samples_render_without_crashing(sample_name: str) -> None:
    sample_dir = RESULTS_DIR / sample_name
    assert sample_dir.exists(), f"Missing PBS sample directory: {sample_dir}"

    completed = _run_sample(sample_name)

    assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
    assert completed.stderr == b""

    rendered = completed.stdout.decode("utf-8", errors="replace")
    assert "Worker Nodes occupancy" in rendered
