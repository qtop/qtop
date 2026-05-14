#!/usr/bin/env python3

import argparse
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Render qtop against historical PBS samples.",
    )
    parser.add_argument(
        "--samples-dir",
        required=True,
        type=Path,
        help="Directory containing qtop-test-repo/qtop5/results samples.",
    )
    parser.add_argument(
        "--min-pass",
        type=int,
        default=100,
        help="Minimum number of samples that must render successfully.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Stop after this many successful samples. Use 0 for all samples.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Optional TSV manifest path to write.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional directory for rendered ANSI text output.",
    )
    return parser.parse_args()


def render_sample(repo_root, sample):
    cmd = [
        sys.executable,
        "-m",
        "qtop_py.cli",
        "-b",
        "pbs",
        "-s",
        str(sample),
        "-c",
        "ON",
        "-F",
        "-r",
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONHASHSEED", "0")
    return subprocess.run(
        cmd,
        cwd=repo_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=25,
    )


def summarize_output(text):
    normalized = normalize_output(text)
    clean = ANSI_RE.sub("", normalized)
    lines = [line for line in clean.splitlines() if line.strip()]
    node_lines = [line for line in lines if "|" in line]
    digest = hashlib.sha256(normalized.encode("utf-8", "replace")).hexdigest()
    return len(lines), len(node_lines), digest, normalized


def normalize_output(text):
    normalized = []
    for line in text.splitlines():
        if line.startswith("Please try it with watch:"):
            normalized.append("Please try it with watch: <command>")
            continue
        line = re.sub(
            r"(===> Job accounting summary <===) .*",
            r"\1 <timestamp>",
            line,
        )
        line = re.sub(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
            "<timestamp>",
            line,
        )
        normalized.append(line.rstrip())
    return "\n".join(normalized) + "\n"


def write_manifest(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("sample\tlines\tnode_lines\tsha256\n")
        for row in rows:
            handle.write(
                "{sample}\t{lines}\t{node_lines}\t{sha256}\n".format(**row),
            )


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    samples = sorted(path for path in args.samples_dir.iterdir() if path.is_dir())
    passed = []
    failed = []

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)

    for sample in samples:
        try:
            result = render_sample(repo_root, sample)
        except subprocess.TimeoutExpired:
            failed.append((sample.name, "timeout"))
            continue

        lines, node_lines, digest, normalized_output = summarize_output(result.stdout)
        if result.returncode == 0 and lines >= 5:
            row = {
                "sample": sample.name,
                "lines": lines,
                "node_lines": node_lines,
                "sha256": digest,
            }
            passed.append(row)
            if args.output_dir:
                output_path = args.output_dir / "{}.ansi.txt".format(sample.name)
                output_path.write_text(normalized_output, encoding="utf-8")
            if args.limit and len(passed) >= args.limit:
                break
        else:
            stderr = " ".join(result.stderr.split())
            failed.append((sample.name, stderr[:240]))

    if args.manifest:
        write_manifest(args.manifest, passed)

    print("Rendered {} PBS samples successfully.".format(len(passed)))
    if failed:
        print("{} samples did not render cleanly before the pass limit.".format(len(failed)))
        for sample, reason in failed[:10]:
            print("{}: {}".format(sample, reason))

    if len(passed) < args.min_pass:
        print(
            "Expected at least {} passing samples, got {}.".format(
                args.min_pass,
                len(passed),
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
