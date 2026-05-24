#!/usr/bin/env python3
"""Render bundled Slurm command-trace samples and save qtop output.

Usage:
    python tools/validate_slurm_samples.py tests/plugins/slurm_samples --output /tmp/qtop-slurm-rendered
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

QTOP_RUNNER = """
import os
import signal
import subprocess
import sys
import types

if os.name == "nt":
    if not hasattr(signal, "SIGPIPE"):
        signal.SIGPIPE = signal.SIGTERM

    termios = types.ModuleType("termios")
    termios.TCSADRAIN = 1
    termios.ECHO = 8
    termios.ICANON = 2
    termios.tcgetattr = lambda file_descriptor: [0, 0, 0, 0, 0, 0]
    termios.tcsetattr = lambda file_descriptor, when, attrs: None
    sys.modules.setdefault("termios", termios)

    popen = subprocess.Popen
    call = subprocess.call

    class SttySize:
        def communicate(self):
            return b"40 120", b""

    class EmptyLookup:
        def communicate(self, input_text=None):
            return "", ""

    def portable_popen(command, *args, **kwargs):
        if command == ["/bin/stty", "size"]:
            return SttySize()
        if command and command[0] in ("cat", "getent"):
            return EmptyLookup()
        return popen(command, *args, **kwargs)

    def portable_call(command, *args, **kwargs):
        if command and command[0] == "/bin/cat":
            target = kwargs.get("stdout") or sys.stdout
            with open(command[1], "r") as handle:
                target.write(handle.read())
            return 0
        return call(command, *args, **kwargs)

    subprocess.Popen = portable_popen
    subprocess.call = portable_call

from qtop_py.qtop import main

sys.argv = ["qtop"] + sys.argv[1:]
raise SystemExit(main())
"""


def sample_cluster_cores(sample_dir):
    from qtop_py.plugins.slurm import SlurmStatExtractor

    extractor = SlurmStatExtractor({}, type("Options", (object,), {"ANONYMIZE": False})())
    nodes_by_name = {}
    for node in extractor.extract_sinfo(str(sample_dir / "sinfo.txt")):
        nodes_by_name[node["NodeName"]] = max(nodes_by_name.get(node["NodeName"], 0), node["np"])
    return sum(nodes_by_name.values())


def render_sample(sample_dir, output_dir):
    savepath = output_dir / "qtop-save" / sample_dir.name
    savepath.mkdir(parents=True, exist_ok=True)
    config_file = output_dir / ("qtop-%s.yaml" % sample_dir.name)
    config_file.write_text("savepath: %s\n" % str(savepath).replace("\\", "/"))

    proc = subprocess.run(
        [sys.executable, "-c", QTOP_RUNNER, "-f", str(config_file), "-e", "-A", "-b", "slurm", "-s", str(sample_dir), "-c", "ON"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=8,
    )
    stdout = proc.stdout.replace(str(REPO_ROOT), "<qtop-repo>").replace(str(REPO_ROOT).replace("\\", "/"), "<qtop-repo>")
    if proc.returncode != 0 or not stdout.strip():
        raise RuntimeError(
            "Slurm sample failed to render: %s\n%s" % (sample_dir, proc.stderr.strip())
        )

    output_file = output_dir / ("%s.ans" % sample_dir.name)
    output_file.write_text(stdout)
    return {
        "sample": sample_dir.name,
        "cluster_cores": sample_cluster_cores(sample_dir),
        "output": output_file.name,
        "stderr_tail": proc.stderr.splitlines()[-5:],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("samples_dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("/tmp/qtop-slurm-rendered"))
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    manifest = []

    for sample_dir in sorted(path for path in args.samples_dir.iterdir() if path.is_dir()):
        entry = render_sample(sample_dir, args.output)
        if entry["cluster_cores"] <= 256:
            raise RuntimeError("Slurm sample is below 257 cores: %s" % sample_dir)
        manifest.append(entry)

    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print("validated=%s output=%s" % (len(manifest), args.output))
    return 0 if len(manifest) == 3 else 1


if __name__ == "__main__":
    raise SystemExit(main())
