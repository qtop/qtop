#!/usr/bin/env python3
##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2026 Vadik Malik
##
## SPDX-License-Identifier: MIT
##
"""Full-tree text-trust audit for repo sanity and trustability (#488).

Complements tools/fortifications.py: fortifications guards the *diff* of a
merge request, while this tool audits the *entire tracked tree* so trojan-
source style payloads cannot ride in via history, generated files, or paths
a reviewer never opened.

Checks and severities:

- CRITICAL  bidirectional control characters (Trojan Source, CVE-2021-42574),
            zero-width/invisible characters, C0/C1 control characters other
            than tab/newline/carriage-return, unicode line/paragraph
            separators, undecodable (non-UTF-8) files.
- WARNING   homoglyph-prone letters (Greek/Cyrillic/fullwidth) in an
            otherwise ASCII tree, byte-order marks.
- INFO      remaining non-ASCII codepoints (reported, never fatal), lines
            that change under NFKC normalisation, CRLF line endings, and
            aggregated ANSI escape/carriage-return counts inside declared
            terminal-output fixtures (see ANSI_FIXTURES), where ESC is the
            point of the file rather than a smell.

Exit status: 1 if any CRITICAL finding (or WARNING with --strict), else 0.
The text report caps identical (file, kind) detail at REPORT_CAP entries and
prints a suppression note, so fixture-heavy trees stay reviewable.

--selftest plants known-bad samples in a temporary directory and proves the
detector catches them; the proof output is suitable for review evidence.

This file is intentionally pure ASCII; every audited codepoint is spelled
as an escape sequence so the auditor passes its own audit.
"""

import argparse
import json
import os
import subprocess
import tempfile
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

BIDI_CONTROLS = set("\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069\u200e\u200f\u061c")
INVISIBLES = set("\u200b\u200c\u200d\u2060\u00ad\u034f\u180e\ufeff")
LINE_SEPARATORS = set("\u2028\u2029\u0085")
HOMOGLYPH_RANGES = ((0x0370, 0x03FF), (0x0400, 0x04FF), (0xFF01, 0xFF5E))
BINARY_SUFFIXES = set(
    [
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".pdf",
        ".gz",
        ".xz",
        ".lzma",
        ".bz2",
        ".zip",
        ".whl",
        ".pyc",
        ".bin",
        ".dat",
        ".woff",
        ".woff2",
    ]
)
SKIP_DIRS = set(
    [
        ".git",
        ".tox",
        ".venv",
        "venv",
        "__pycache__",
        "artifacts",
        "build",
        "dist",
        "qtop.egg-info",
        ".pytest_cache",
        ".ruff_cache",
    ]
)
MAX_BYTES = 5 * 1024 * 1024
REPORT_CAP = 20

# Files whose entire purpose is captured terminal output, where ANSI escape
# sequences and bare carriage returns are expected and aggregate to a single
# INFO finding instead of thousands of control-character CRITICALs. Any OTHER
# control/bidi/invisible character still escalates normally inside them.
#   - qtop_py/contrib/  : recorded scheduler render fixtures (the *.ref files
#                         the sample gates diff against) -- full of ESC colour
#                         codes by design.
#   - helpfile.txt      : qtop's in-app help screen, shipped as pre-rendered
#                         terminal text (ANSI-coloured) and printed verbatim
#                         by the tool, so it legitimately contains ESC codes.
ANSI_FIXTURES = ("qtop_py/contrib/", "helpfile.txt")


def is_ansi_fixture(rel):
    """Return True if ``rel`` is a declared terminal-output fixture path.

    Matches an ANSI_FIXTURES entry exactly or as a directory prefix, so both
    ``helpfile.txt`` (exact) and everything under ``qtop_py/contrib/`` count.
    """
    for prefix in ANSI_FIXTURES:
        if rel == prefix or rel.startswith(prefix):
            return True
    return False


def tracked_files(root):
    """Yield every file git tracks under ``root`` (falls back to os.walk).

    Using ``git ls-files`` keeps the audit aligned with what is actually
    committed (ignoring untracked scratch files); if git is unavailable the
    os.walk fallback skips SKIP_DIRS so results stay comparable.
    """
    try:
        out = subprocess.check_output(["git", "ls-files", "-z"], cwd=str(root), stderr=subprocess.DEVNULL)
        names = [n for n in out.decode("utf-8", "replace").split("\0") if n]
        return [root / n for n in names]
    except (OSError, subprocess.CalledProcessError):
        found = []
        for dirpath, dirnames, filenames in os.walk(str(root)):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for filename in filenames:
                found.append(Path(dirpath) / filename)
        return found


def codepoint_label(ch):
    """Return a human-readable ``U+XXXX NAME`` label for a character.

    Falls back to ``UNNAMED`` for codepoints unicodedata cannot name, so the
    report never crashes on an obscure control character.
    """
    try:
        name = unicodedata.name(ch)
    except ValueError:
        name = "UNNAMED"
    return "U+%04X %s" % (ord(ch), name)


def is_homoglyph_risk(ch):
    """Return True if ``ch`` falls in a homoglyph-prone Unicode range.

    Covers Greek, Cyrillic, and fullwidth Latin blocks (HOMOGLYPH_RANGES) --
    letters that look like ASCII but are not, a common spoofing vector.
    """
    cp = ord(ch)
    for lo, hi in HOMOGLYPH_RANGES:
        if lo <= cp <= hi:
            return True
    return False


def proof_line(text, col):
    """Render an escaped one-line excerpt with a caret under column ``col``.

    Long lines are windowed around the offending column so the proof stays
    readable; the text is unicode-escaped so the report itself is pure ASCII.
    """
    visual = text
    if len(visual) > 160:
        start = max(0, col - 40)
        visual = visual[start : start + 120]
        col = col - start
    escaped = visual.encode("unicode_escape").decode("ascii")
    return "    >%s\n    %s^" % (escaped, " " * (col + 1))


def scan_file(path, rel, findings):
    """Scan one file and append (severity, path, line, col, msg, proof) rows.

    Skips binary content and oversized files, decodes as UTF-8 (a decode
    failure is itself a CRITICAL finding), then classifies every non-plain-ASCII
    codepoint by severity. Inside ANSI_FIXTURES, ESC and bare CR are tallied
    into one expected-INFO row instead of flooding the report.
    """
    try:
        data = path.read_bytes()
    except OSError as exc:
        findings.append(("CRITICAL", rel, 0, 0, "unreadable file: %s" % exc, ""))
        return
    if not data:
        return
    if len(data) > MAX_BYTES:
        findings.append(("INFO", rel, 0, 0, "skipped: larger than %d bytes" % MAX_BYTES, ""))
        return
    if b"\0" in data[:8000]:
        return  # binary content, out of scope for text trust
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        findings.append(("CRITICAL", rel, 0, 0, "not valid UTF-8: %s" % exc, ""))
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
            o = ord(ch)
            if ch == "\t" or 0x20 <= o <= 0x7E:
                continue
            if ansi_fixture and ch == "\x1b":
                esc_count += 1
                continue
            if ansi_fixture and ch == "\r":
                cr_count += 1
                continue
            if ch == "\ufeff" and lineno == 1 and col == 0:
                findings.append(("WARNING", rel, lineno, col, "leading byte-order mark", proof_line(line, col)))
                continue
            label = codepoint_label(ch)
            if ch in BIDI_CONTROLS:
                findings.append(("CRITICAL", rel, lineno, col, "bidirectional control character %s (Trojan Source)" % label, proof_line(line, col)))
            elif ch in INVISIBLES:
                findings.append(("CRITICAL", rel, lineno, col, "invisible character %s" % label, proof_line(line, col)))
            elif ch in LINE_SEPARATORS:
                findings.append(("CRITICAL", rel, lineno, col, "unicode line separator %s" % label, proof_line(line, col)))
            elif o < 0x20 or o == 0x7F or 0x80 <= o <= 0x9F:
                findings.append(("CRITICAL", rel, lineno, col, "control character %s" % label, proof_line(line, col)))
            elif is_homoglyph_risk(ch):
                findings.append(("WARNING", rel, lineno, col, "homoglyph-prone character %s" % label, proof_line(line, col)))
            else:
                findings.append(("INFO", rel, lineno, col, "non-ASCII character %s" % label, ""))
        nfkc = unicodedata.normalize("NFKC", line)
        if nfkc != line:
            findings.append(("INFO", rel, lineno, 0, "line changes under NFKC normalisation", ""))
    if esc_count or cr_count:
        findings.append(("INFO", rel, 0, 0, "terminal-output fixture: %d ANSI escapes, %d carriage returns (expected)" % (esc_count, cr_count), ""))


def write_reports(findings, report_dir, scanned):
    """Write report.txt + findings.json and return (counts, report_text).

    Per-line detail is capped at REPORT_CAP entries for each (file, kind) so a
    fixture-heavy tree stays reviewable; suppressed extras are summarised in a
    NOTE line. findings.json always carries the full, uncapped list.
    """
    report_dir.mkdir(parents=True, exist_ok=True)
    counts = {"CRITICAL": 0, "WARNING": 0, "INFO": 0}
    lines = []
    lines.append("repo-sanity text-trust audit")
    lines.append("root: %s" % ROOT)
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
    (report_dir / "report.txt").write_text(report, encoding="utf-8")
    payload = [{"severity": s, "path": p, "line": ln, "col": c, "message": m} for s, p, ln, c, m, _ in findings]
    (report_dir / "findings.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return counts, report


def run_audit(root, report_dir, strict):
    """Audit every tracked text file under ``root`` and return an exit code.

    Prints non-INFO findings to stdout, always writes the full report, and
    returns 1 on any CRITICAL (or on WARNING when ``strict`` is set), else 0.
    """
    findings = []
    scanned = 0
    for path in sorted(tracked_files(root)):
        if not path.is_file():
            continue
        if path.suffix.lower() in BINARY_SUFFIXES:
            continue
        rel = str(path.relative_to(root))
        if any(part in SKIP_DIRS for part in Path(rel).parts):
            continue
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
    """Plant one file per payload class and prove each is detected.

    Writes bidi/zero-width/homoglyph/control/line-separator samples to a temp
    dir, scans them, and fails (returns 1) if any planted file escapes with no
    CRITICAL/WARNING -- so the detector's own correctness is demonstrable.
    """
    tmp = Path(tempfile.mkdtemp(prefix="repo-sanity-selftest-"))
    plants = {
        "planted_bidi.py": 'access = "user\u202e" # privileged \u202d"\n',
        "planted_zwsp.py": "def is\u200badmin():\n    return True\n",
        "planted_homoglyph.py": "p\u0430ssword_check = None  # Cyrillic small a\n",
        "planted_control.py": "header = 'x\x08x'\n",
        "planted_separator.py": "safe = True\u2028unsafe = True\n",
    }
    for name, payload in plants.items():
        (tmp / name).write_text(payload, encoding="utf-8")
    findings = []
    for path in sorted(tmp.iterdir()):
        scan_file(path, str(path.name), findings)
    counts, report = write_reports(findings, report_dir, len(plants))
    print(report)
    hit = set(f[1] for f in findings if f[0] in ("CRITICAL", "WARNING"))
    missing = [name for name in sorted(plants) if name not in hit]
    if missing:
        print("SELFTEST FAILED: undetected plants: %s" % ", ".join(missing))
        return 1
    print("SELFTEST OK: all %d planted payloads detected (proof above)" % len(plants))
    return 0


def parse_args():
    """Parse CLI options (--report-dir, --strict, --selftest)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", default="artifacts/repo-sanity", help="Directory for report.txt and findings.json")
    parser.add_argument("--strict", action="store_true", help="Treat WARNING findings as fatal")
    parser.add_argument("--selftest", action="store_true", help="Plant known-bad samples and prove detection")
    return parser.parse_args()


def main():
    """Entry point: run --selftest, or audit the repo and return its code."""
    args = parse_args()
    report_dir = Path(args.report_dir)
    if args.selftest:
        return selftest(report_dir)
    return run_audit(ROOT, report_dir, args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
