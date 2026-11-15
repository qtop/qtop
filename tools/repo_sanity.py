#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Vadik Malik
##
## SPDX-License-Identifier: MIT
##
"""Audit tracked text and branch diffs without relaxing terminal-fixture exceptions.

Critical findings fail; --strict also fails warnings. JSON retains every finding,
while text caps repeated details. --selftest proves the planted attack classes.
Ruff separately checks Python syntax, eval, exec, shell execution and imports.
"""

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import tempfile
import unicodedata
from pathlib import Path

try:
    from tools.private_output import private_output_dir, write_private_file
except ImportError:
    from private_output import private_output_dir, write_private_file

ROOT = Path(__file__).resolve().parents[1]

BIDI_CONTROLS = set("\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069\u200e\u200f\u061c")
INVISIBLES = set("\u200b\u200c\u200d\u2060\u00ad\u034f\u180e\ufeff")
LINE_SEPARATORS = set("\u2028\u2029\u0085")
HOMOGLYPH_RANGES = ((0x0370, 0x03FF), (0x0400, 0x04FF), (0xFF01, 0xFF5E))
BINARY_SUFFIXES = set(".png .jpg .jpeg .gif .webp .ico .pdf .gz .xz .lzma .bz2 .zip .whl .pyc .bin .dat .woff .woff2".split())
MAX_BYTES = 5 * 1024 * 1024
REPORT_CAP = 20

# Exact terminal-output fixtures allowed to contain ANSI SGR colour sequences.
# Other escape commands, including cursor controls and OSC payloads, remain
# CRITICAL even in these files.
ANSI_FIXTURES = set(
    [
        "qtop_py/contrib/oar1_dvv_out.ref",
        "qtop_py/contrib/pbs_dvv_out.ref",
        "qtop_py/contrib/sger_dvv_out.ref",
        "helpfile.txt",
    ]
)
ANSI_SGR_RE = re.compile(r"\x1b\[[0-9;]{0,64}m")
LEGACY_CR_FIXTURES = ANSI_FIXTURES.difference(set(["helpfile.txt"]))


CONTROL_OR_BIDI = re.compile(r"[^\x09\x0a\x0d\x20-\x7e]")
GENERATED_OR_BINARY = re.compile(
    r"(^|/)(m4|autogen|configure|Makefile\.in|cmake|tests?/files|fixtures?)/|\.(xz|lzma|gz|bin|dat|png|jpg|jpeg|gif|webp)$",
    re.IGNORECASE,
)


def run_git(args):
    """Run git in the repository and return its output."""
    return subprocess.check_output(["git"] + args, cwd=str(ROOT), stderr=subprocess.STDOUT, universal_newlines=True, encoding="utf-8", errors="replace")


def changed_files(base_ref):
    """Return the files changed since base_ref."""
    output = run_git(["diff", "--no-ext-diff", "--name-only", "-z", base_ref, "--"])
    return [name for name in output.split("\0") if name]


def diff_lines(base_ref):
    """Return the lines of the zero-context diff against base_ref."""
    output = run_git(["-c", "core.quotePath=false", "diff", "--no-ext-diff", "--no-textconv", "--no-color", "--src-prefix=a/", "--dst-prefix=b/", "--unified=0", base_ref, "--"])
    return output.split("\n")


def find_diff_problems(lines):
    """Report added lines that contain control, bidi or non-ASCII characters."""
    problems = []
    filename, number = None, None
    for line in lines:
        if line.startswith("diff --git "):
            filename, number = None, None
            continue
        if line.startswith("+++ ") and number is None:
            filename = line[4:]
            if filename.startswith('"'):
                filename = ast.literal_eval(filename)
            filename = filename[2:] if filename.startswith("b/") else filename
            if filename == "/dev/null":
                filename = None
            continue
        hunk = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if hunk:
            number = int(hunk.group(1))
            continue
        if filename is None or number is None:
            continue
        if line.startswith("+"):
            text = line[1:]
            match = CONTROL_OR_BIDI.search(text)
            if match:
                safe_filename = filename.encode("unicode_escape").decode("ascii")
                problems.append(
                    "%s:%s:%s: control, bidi, or non-ASCII character U+%04X in added line: %s" % (safe_filename, number, match.start() + 1, ord(match.group()), ascii(text))
                )
            number += 1
        elif line.startswith(" "):
            number += 1
    return problems


def check_diff(base_ref):
    """Check pending branch edits for non-ASCII text and generated paths."""
    try:
        base = run_git(["merge-base", base_ref, "HEAD"]).strip()
        problems = ["manual review required for generated/binary-looking path: %s" % name for name in changed_files(base) if GENERATED_OR_BINARY.search(name)]
        problems.extend(find_diff_problems(diff_lines(base)))
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.output)
        print("Unable to compare against %s" % base_ref, file=sys.stderr)
        return 2
    for problem in problems:
        print(problem, file=sys.stderr)
    if not problems:
        print("fortifications: ok")
    return int(bool(problems))


class TrackedFileDiscoveryError(RuntimeError):
    """Raised when the tracked Git tree cannot be enumerated safely."""


def is_ansi_fixture(rel):
    """Return True if ``rel`` is an exact declared terminal fixture."""
    return rel in ANSI_FIXTURES


def is_expected_fixture_cr(rel, lineno, col, line):
    """Recognize the single historical leading CR in each scheduler render."""
    return rel in LEGACY_CR_FIXTURES and lineno == 2 and col == 0 and line.startswith("\r./qtop.py ## Queueing System report tool.")


def tracked_files(root):
    """Return every file Git tracks under ``root``."""
    try:
        out = subprocess.check_output(["git", "ls-files", "-z"], cwd=str(root), stderr=subprocess.DEVNULL)
    except OSError as exc:
        raise TrackedFileDiscoveryError("Git is required to enumerate tracked files: %s" % exc) from exc
    except subprocess.CalledProcessError as exc:
        raise TrackedFileDiscoveryError("'git ls-files' failed in %s with exit status %s; run repo-sanity from a valid Git worktree" % (root, exc.returncode)) from exc
    names = [os.fsdecode(name) for name in out.split(b"\0") if name]
    return [root / n for n in names]


def report_path(rel):
    """Return a UTF-8-safe report spelling for a repository-relative path."""
    return rel.encode("utf-8", "backslashreplace").decode("utf-8")


def codepoint_label(ch):
    """Return a human-readable ``U+XXXX NAME`` label for a character."""
    return "U+%04X %s" % (ord(ch), unicodedata.name(ch, "UNNAMED"))


def is_homoglyph_risk(ch):
    """Return True if ``ch`` falls in a homoglyph-prone Unicode range."""
    return any(lo <= ord(ch) <= hi for lo, hi in HOMOGLYPH_RANGES)


def proof_line(text, col):
    """Render an escaped one-line excerpt with a caret under column ``col``."""
    visual = text
    if len(visual) > 160:
        start = max(0, col - 40)
        visual = visual[start : start + 120]
        col = col - start
    escaped = visual.encode("unicode_escape").decode("ascii")
    return "    >%s\n    %s^" % (escaped, " " * (col + 1))


def _read_text(path, rel, findings):
    """Return the file's UTF-8 text, or None when it is unreadable, empty, oversized, binary or not UTF-8."""
    try:
        data = path.read_bytes()
    except OSError as exc:
        findings.append(("CRITICAL", rel, 0, 0, "unreadable file: %s" % exc, ""))
        return None
    if not data:
        return None
    if len(data) > MAX_BYTES:
        findings.append(("INFO", rel, 0, 0, "skipped: larger than %d bytes" % MAX_BYTES, ""))
        return None
    if b"\0" in data[:8000]:
        return None  # binary content, out of scope for text trust
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        findings.append(("CRITICAL", rel, 0, 0, "not valid UTF-8: %s" % exc, ""))
        return None


def _classify(ch, rel, lineno, col, line):
    """The finding row for one non-plain-ASCII codepoint (BOM, bidi, invisible, separator, control, homoglyph, other)."""
    o = ord(ch)
    if ch == "\ufeff" and lineno == 1 and col == 0:
        return ("WARNING", rel, lineno, col, "leading byte-order mark", proof_line(line, col))
    label = codepoint_label(ch)
    if ch in BIDI_CONTROLS:
        return ("CRITICAL", rel, lineno, col, "bidirectional control character %s (Trojan Source)" % label, proof_line(line, col))
    if ch in INVISIBLES:
        return ("CRITICAL", rel, lineno, col, "invisible character %s" % label, proof_line(line, col))
    if ch in LINE_SEPARATORS:
        return ("CRITICAL", rel, lineno, col, "unicode line separator %s" % label, proof_line(line, col))
    if o < 0x20 or o == 0x7F or 0x80 <= o <= 0x9F:
        return ("CRITICAL", rel, lineno, col, "control character %s" % label, proof_line(line, col))
    if is_homoglyph_risk(ch):
        return ("WARNING", rel, lineno, col, "homoglyph-prone character %s" % label, proof_line(line, col))
    return ("INFO", rel, lineno, col, "non-ASCII character %s" % label, "")


def scan_file(path, rel, findings):
    """Scan one file and append (severity, path, line, col, msg, proof) rows."""
    text = _read_text(path, rel, findings)
    if text is None:
        return
    crlf = text.count("\r\n")
    if crlf:
        findings.append(("INFO", rel, 0, 0, "%d CRLF line endings" % crlf, ""))
    ansi_fixture = is_ansi_fixture(rel)
    esc_count = 0
    cr_count = 0
    for lineno, line in enumerate(text.split("\n"), 1):
        if line.endswith("\r"):
            line = line[:-1]
        for col, ch in enumerate(line):
            if ch == "\t" or 0x20 <= ord(ch) <= 0x7E:
                continue
            if ansi_fixture and ch == "\x1b" and ANSI_SGR_RE.match(line, col):
                esc_count += 1
                continue
            if ansi_fixture and ch == "\r" and is_expected_fixture_cr(rel, lineno, col, line):
                cr_count += 1
                continue
            findings.append(_classify(ch, rel, lineno, col, line))
        nfkc = unicodedata.normalize("NFKC", line)
        if nfkc != line:
            findings.append(("INFO", rel, lineno, 0, "line changes under NFKC normalisation", ""))
    if esc_count or cr_count:
        findings.append(("INFO", rel, 0, 0, "terminal-output fixture: %d ANSI SGR sequences, %d historical carriage returns" % (esc_count, cr_count), ""))


def write_reports(findings, report_dir, scanned):
    """Write report.txt + findings.json and return (counts, report_text)."""
    report_dir = private_output_dir(report_dir)
    counts = {"CRITICAL": 0, "WARNING": 0, "INFO": 0}
    lines = []
    lines.append("repo-sanity text-trust audit")
    lines.append("root: <repo>")
    lines.append("files scanned: %d" % scanned)
    lines.append("")
    shown = {}
    suppressed = {}
    for sev, rel, lineno, col, msg, proof in findings:
        counts[sev] = counts.get(sev, 0) + 1
        kind = msg.split(" U+")[0]
        key = (rel, kind)
        shown[key] = shown.get(key, 0) + 1
        if shown[key] > REPORT_CAP:
            suppressed[key] = suppressed.get(key, 0) + 1
            continue
        lines.append("%-8s %s:%s:%s  %s" % (sev, rel, lineno, col + 1, msg))
        if proof:
            lines.append(proof)
    for (rel, kind), extra in sorted(suppressed.items()):
        lines.append("NOTE     %s: %d further '%s' findings suppressed (cap %d)" % (rel, extra, kind, REPORT_CAP))
    lines.append("")
    lines.append("summary: %d critical, %d warning, %d info" % (counts["CRITICAL"], counts["WARNING"], counts["INFO"]))
    report = "\n".join(lines) + "\n"
    write_private_file(report_dir / "report.txt", report)
    payload = [{"severity": s, "path": p, "line": ln, "col": c, "message": m} for s, p, ln, c, m, _ in findings]
    write_private_file(report_dir / "findings.json", json.dumps(payload, indent=2) + "\n")
    return counts, report


def run_audit(root, report_dir, strict):
    """Audit every tracked text file under ``root`` and return an exit code."""
    findings = []
    scanned = 0
    for path in sorted(tracked_files(root)):
        if not path.is_file():
            continue
        if path.suffix.lower() in BINARY_SUFFIXES:
            continue
        rel = report_path(path.relative_to(root).as_posix())
        scanned += 1
        scan_file(path, rel, findings)
    counts, report = write_reports(findings, report_dir, scanned)
    for line in report.splitlines():
        if not line.startswith("INFO"):
            print(line)
    print("full report: %s" % (report_dir / "report.txt"))
    if counts["CRITICAL"]:
        return 1
    if strict and counts["WARNING"]:
        return 1
    return 0


def selftest(report_dir):
    """Plant one file per payload class and prove each is detected."""
    with tempfile.TemporaryDirectory(prefix="repo-sanity-selftest-") as tmp_name:
        tmp = Path(tmp_name)
        plants = {
            "planted_bidi.py": 'access = "user\u202e" # privileged \u202d"\n',
            "planted_zwsp.py": "def is\u200badmin():\n    return True\n",
            "planted_homoglyph.py": "p\u0430ssword_check = None  # Cyrillic small a\n",
            "planted_control.py": "header = 'x\x08x'\n",
            "planted_separator.py": "safe = True\u2028unsafe = True\n",
            "qtop_py/contrib/oar1_dvv_out.ref": "\x1b]52;c;Y2xpcGJvYXJk\x07\n",
            "qtop_py/contrib/pbs_dvv_out.ref": "visible text\rconcealed text\n",
            "helpfile.txt": "\x1b[2Jconcealed heading\n",
        }
        for rel, payload in plants.items():
            path = tmp / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload, encoding="utf-8")
        findings = []
        for rel in sorted(plants):
            scan_file(tmp / rel, rel, findings)
        safe_rel = "qtop_py/contrib/sger_dvv_out.ref"
        safe_path = tmp / safe_rel
        safe_path.parent.mkdir(parents=True, exist_ok=True)
        safe_path.write_text("\x1b[1;32mgreen\x1b[0;m\n", encoding="utf-8")
        scan_file(safe_path, safe_rel, findings)
        _counts, report = write_reports(findings, report_dir, len(plants) + 1)
        print(report)
        hit = set(f[1] for f in findings if f[0] in ("CRITICAL", "WARNING"))
        missing = [name for name in sorted(plants) if name not in hit]
        if missing:
            print("SELFTEST FAILED: undetected plants: %s" % ", ".join(missing))
            return 1
        safe_failures = [finding for finding in findings if finding[1] == safe_rel and finding[0] in ("CRITICAL", "WARNING")]
        if safe_failures:
            print("SELFTEST FAILED: valid ANSI SGR sequence was rejected")
            return 1
        print("SELFTEST OK: all %d planted payloads detected (proof above)" % len(plants))
        return 0


def parse_args():
    """Parse CLI options (--report-dir, --strict, --selftest)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", help="Check branch diff only, against this merge base")
    parser.add_argument("--report-dir", default="artifacts/repo-sanity", help="Directory for report.txt and findings.json")
    parser.add_argument("--strict", action="store_true", help="Treat WARNING findings as fatal")
    parser.add_argument("--selftest", action="store_true", help="Plant known-bad samples and prove detection")
    return parser.parse_args()


def main():
    """Entry point: run --selftest, or audit the repo and return its code."""
    args = parse_args()
    if args.base_ref:
        return check_diff(args.base_ref)
    report_dir = Path(args.report_dir)
    if args.selftest:
        return selftest(report_dir)
    try:
        return run_audit(ROOT, report_dir, args.strict)
    except TrackedFileDiscoveryError as exc:
        print("repo-sanity error: %s" % exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
