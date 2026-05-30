#!/usr/bin/env python3
"""Lightweight repository health checks for CI."""

import os
import subprocess
import sys


BIDI_CONTROLS = set(
    [
        "\u202a",
        "\u202b",
        "\u202c",
        "\u202d",
        "\u202e",
        "\u2066",
        "\u2067",
        "\u2068",
        "\u2069",
    ]
)

GENERATED_PATH_PARTS = set(["build", "dist", ".cache", "__pycache__", ".pytest_cache"])
GENERATED_SUFFIXES = (".pyc", ".pyo", ".gz", ".xz", ".lzma", ".bin", ".dat")
CONTROL_CHAR_EXEMPTIONS = set(
    [
        "helpfile.txt",
        "qtop_py/contrib/oar1_dvv_out.ref",
        "qtop_py/contrib/pbs_dvv_out.ref",
        "qtop_py/contrib/sger_dvv_out.ref",
    ]
)


def git_ls_files():
    proc = subprocess.Popen(["git", "ls-files"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        sys.stderr.write(stderr.decode("utf-8", "replace"))
        raise SystemExit(proc.returncode)
    return stdout.decode("utf-8").splitlines()


def has_unwanted_control_char(text):
    for char in text:
        codepoint = ord(char)
        if char in BIDI_CONTROLS:
            return True
        if codepoint < 32 and char not in ("\t", "\n", "\r"):
            return True
    return False


def is_generated_path(path):
    parts = set(path.split(os.sep))
    return bool(parts & GENERATED_PATH_PARTS) or path.endswith(GENERATED_SUFFIXES)


def main():
    failures = []
    for path in git_ls_files():
        if is_generated_path(path):
            failures.append("generated artifact is tracked: %s" % path)
            continue
        if path in CONTROL_CHAR_EXEMPTIONS:
            continue

        try:
            with open(path, "rb") as handle:
                raw = handle.read()
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        except OSError as exc:
            failures.append("could not read %s: %s" % (path, exc))
            continue

        if has_unwanted_control_char(text):
            failures.append("unexpected control or bidi character in: %s" % path)

    if failures:
        sys.stderr.write("\n".join(failures) + "\n")
        return 1

    print("Fortifications OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
