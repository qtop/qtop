# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Seed a persistent local fuzz corpus without replacing discovered inputs."""

import argparse
from pathlib import Path

from tools.private_output import private_output_dir


def main() -> None:
    """Decode reviewed seed documents into the private fuzz corpus."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    output = private_output_dir(args.directory)
    seeds = Path(__file__).resolve().parents[1] / "tests" / "fuzz_corpus"
    for seed in sorted(seeds.glob("*.hex")):
        target = output / seed.stem
        if not target.exists():
            target.write_bytes(bytes.fromhex(seed.read_text()))


if __name__ == "__main__":
    main()
