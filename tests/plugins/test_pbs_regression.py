import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py.plugins.pbs import PBSStatExtractor
from qtop_py.qtop import WNOccupancy


def test_qstat_regex_skips_unparseable_lines(tmp_path):
    qstat = tmp_path / "qstat.txt"
    qstat.write_text(
        "Job id            Name             User              Time Use S Queue\n"
        "----------------  ---------------- ----------------  -------- - -----\n"
        "this line is not a qstat row\n"
        "1234.server       job              alice             00:00:01 R workq\n"
    )

    extractor = PBSStatExtractor(config={}, options=SimpleNamespace(ANONYMIZE=False))

    assert extractor._extract_qstat_regex(str(qstat)) == [
        {"JobId": "1234", "UnixAccount": "alice", "S": "R", "Queue": "workq"}
    ]


def test_valid_corejobs_skips_jobs_missing_from_qstat():
    occupancy = WNOccupancy.__new__(WNOccupancy)

    result = list(occupancy._valid_corejobs({"0": "missing", "1": "known"}, {"known": ("alice", "workq")}))

    assert result == [("alice", "1", "workq")]


@pytest.mark.skipif(not os.environ.get("QTOP_PBS_SAMPLE_DIR"), reason="set QTOP_PBS_SAMPLE_DIR to run external PBS sample regression")
def test_external_pbs_samples_render_without_crashing(tmp_path):
    sample_dir = Path(os.environ["QTOP_PBS_SAMPLE_DIR"])
    samples = [path for path in sample_dir.iterdir() if path.is_dir()]
    assert len(samples) >= 100

    repo_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "home")

    failures = []
    for sample in samples[:100]:
        result = subprocess.run(
            [sys.executable, "-m", "qtop_py.cli", "-b", "pbs", "-s", str(sample), "-c", "ON"],
            cwd=repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        if result.returncode:
            failures.append((sample.name, result.stderr[-1000:], result.stdout[-1000:]))

    assert failures == []
