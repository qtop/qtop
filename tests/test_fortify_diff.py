import importlib.util
import sys
from pathlib import Path


def load_fortify_diff():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "fortify_diff.py"
    spec = importlib.util.spec_from_file_location("fortify_diff", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


fortify_diff = load_fortify_diff()


def test_artifact_paths_flags_generated_outputs():
    paths = [
        "dist/qtop-1.0.tar.gz",
        "qtop.egg-info/PKG-INFO",
        "build/lib/qtop.py",
        "qtop_py/plugins/slurm.py",
    ]

    assert fortify_diff.artifact_paths(paths) == paths[:3]


def test_suspicious_added_lines_flags_non_ascii_and_controls():
    diff = "\n".join(
        [
            "diff --git a/file b/file",
            "+++ b/file",
            "+plain ascii",
            "+unicode caf\u00e9",
            "+bidi \u202e marker",
            "+control \x01 marker",
        ]
    )

    findings = fortify_diff.suspicious_added_lines(diff)

    assert [finding.reason for finding in findings] == [
        "non-ascii character",
        "bidirectional unicode control",
        "control character",
    ]


def test_suspicious_added_lines_can_allow_non_ascii_text():
    diff = "+unicode caf\u00e9\n+bidi \u202e marker"

    findings = fortify_diff.suspicious_added_lines(diff, allow_non_ascii=True)

    assert [finding.reason for finding in findings] == ["bidirectional unicode control"]
