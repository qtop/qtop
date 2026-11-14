# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Exercise a real watch-mode terminal and report bounded soak-test evidence."""

import argparse
import contextlib
import json
import os
import select
import signal
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]


def _resident_kib(pid: int) -> int:
    status = Path("/proc/%s/status" % pid)
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def _revision() -> Optional[str]:
    """Return the revision when Git can read this checkout."""
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(ROOT), universal_newlines=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def probe(directory: Path, duration: float, backend: str = "demo", source: Optional[str] = None, shutdown: str = "q") -> Dict[str, Any]:
    """Run qtop in a PTY, exercise keys/resize, and verify terminal restoration."""
    if os.name != "posix":
        raise RuntimeError("PTY checks require POSIX; on Windows use WSL")
    import pty

    with contextlib.ExitStack() as terminals:
        master, slave = pty.openpty()
        terminals.callback(os.close, slave)
        terminals.callback(os.close, master)
        return _probe(master, slave, directory, duration, backend, source, shutdown)


def _probe(master: int, slave: int, directory: Path, duration: float, backend: str, source: Optional[str], shutdown: str) -> Dict[str, Any]:
    """Exercise a watch process while the caller owns its terminal descriptors."""
    import fcntl
    import termios

    previous = termios.tcgetattr(slave)
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 60, 160, 0, 0))
    home = directory / "home"
    home.mkdir()
    output = directory / "output"
    env = os.environ.copy()
    env.update(HOME=str(home), TERM="xterm", QTOP_COLOR="ON")
    env.pop("QTOP_SCHEDULER", None)
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", backend, "-w", "1", "-c", "ON", "-L", "-E", "-o", "savepath=" + str(output)]
    if source:
        command.extend(["-s", source])
    process = subprocess.Popen(command, cwd=str(ROOT), env=env, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
    transcript = bytearray()
    frame_times = []
    peak_rss_kib = peak_files = 0
    key_sent = False
    exercised = False
    started = time.monotonic()
    deadline = started + 20
    try:
        while True:
            if select.select([master], [], [], 0.05)[0]:
                transcript.extend(os.read(master, 65536))
            if len(transcript) > 16 * 1024 * 1024:
                raise AssertionError("terminal output exceeded 16 MiB budget")
            frames = transcript.count(b"\x1b[2J")
            while len(frame_times) < frames:
                frame_times.append(time.monotonic())
            if not key_sent and b"Job accounting summary" in transcript:
                attrs = termios.tcgetattr(slave)
                assert not attrs[3] & (termios.ECHO | termios.ICANON), "watch terminal is not in cbreak mode"
                os.write(master, b"F")
                key_sent = True
                deadline = time.monotonic() + 20
            if key_sent and not exercised and b"Toggling full-name" in transcript:
                # A resize signal can interrupt stdin.read(1). Wait for the
                # key's observable effect before exercising resize separately.
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 45, 120, 0, 0))
                os.kill(process.pid, signal.SIGWINCH)
                exercised = True
                deadline = time.monotonic() + duration
            peak_rss_kib = max(peak_rss_kib, _resident_kib(process.pid))
            if output.exists():
                peak_files = max(peak_files, sum(1 for path in output.rglob("*") if path.is_file()))
            if process.poll() is not None:
                raise AssertionError("watch exited before shutdown: " + transcript[-4000:].decode("utf-8", "replace"))
            if time.monotonic() >= deadline:
                break
        assert exercised and len(frame_times) >= 2, "watch did not acknowledge input and refresh: " + transcript[-4000:].decode("utf-8", "replace")
        assert b"Toggling full-name" in transcript, "single-key input did not reach the watch loop"
        if shutdown == "q":
            os.write(master, b"q")
        else:
            os.kill(process.pid, getattr(signal, shutdown))
        # Drain during shutdown, so writing a final frame cannot fill the PTY.
        deadline = time.monotonic() + 10
        while process.poll() is None and time.monotonic() < deadline:
            if select.select([master], [], [], 0.05)[0]:
                transcript.extend(os.read(master, 65536))
        assert process.poll() is not None, "watch did not exit within 10 seconds"
        expected = 128 + signal.SIGTERM if shutdown == "SIGTERM" else 0
        assert process.returncode == expected, transcript[-4000:].decode("utf-8", "replace")
        assert termios.tcgetattr(slave) == previous, "terminal attributes were not restored"
        assert b"Traceback" not in transcript, transcript[-4000:].decode("utf-8", "replace")
        snapshots = sorted(output.glob("qtop_json_*.json"))
        assert snapshots, "watch did not export any snapshots"
        for snapshot in snapshots:
            state = json.loads(snapshot.read_text())
            assert all(key in state for key in ("jobs", "nodes", "queues"))
        archives = sorted(output.glob("*.tar"))
        assert archives, "watch did not produce support captures"
        # Raw captures are scheduler traces, not portable snapshots; inspect archive integrity.
        import tarfile

        for archive in archives:
            with tarfile.open(str(archive)) as captured:
                assert captured.getmembers(), "empty capture archive"
        return {
            "revision": _revision(),
            "python": sys.version,
            "backend": backend,
            "source": source,
            "shutdown": shutdown,
            "elapsed_seconds": time.monotonic() - started,
            "frames": len(frame_times),
            "max_frame_gap_seconds": max(b - a for a, b in zip(frame_times, frame_times[1:])),
            "peak_rss_kib": peak_rss_kib,
            "peak_output_files": peak_files,
            "snapshots": len(snapshots),
            "captures": len(archives),
            "terminal_restored": True,
        }
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        termios.tcsetattr(slave, termios.TCSANOW, previous)
        (directory / "terminal.ans").write_bytes(transcript)


def main() -> None:
    """Run a fixture or explicitly requested live soak and retain a JSON report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--backend", choices=("demo", "slurm", "pbs", "sge", "oar"), default="demo")
    parser.add_argument("--source", type=str)
    parser.add_argument("--live", action="store_true", help="Allow contacting a live scheduler")
    parser.add_argument("--max-rss-mib", type=int, default=256)
    parser.add_argument("--max-files", type=int, default=1000)
    parser.add_argument("--max-frame-gap", type=float, default=10)
    parser.add_argument("--report", type=Path, default=Path("artifacts/soak/report.json"))
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("PTY checks require POSIX; on Windows use WSL")
    if args.duration < 2 or args.duration > 86400:
        parser.error("duration must be between 2 and 86400 seconds")
    if args.backend != "demo" and not (args.source or args.live):
        parser.error("non-demo backends require --source or explicit --live")
    from tools.private_output import private_output_dir

    private_output_dir(args.report.parent)
    with tempfile.TemporaryDirectory(prefix="qtop-soak-") as directory:
        result = probe(Path(directory), args.duration, args.backend, str(Path(args.source).resolve()) if args.source else None)
        result["limits"] = {"rss_mib": args.max_rss_mib, "files": args.max_files, "frame_gap_seconds": args.max_frame_gap}
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        args.report.with_suffix(".ans").write_bytes((Path(directory) / "terminal.ans").read_bytes())
        assert result["peak_rss_kib"] <= args.max_rss_mib * 1024, "RSS budget exceeded"
        assert result["peak_output_files"] <= args.max_files, "file budget exceeded"
        assert result["max_frame_gap_seconds"] <= args.max_frame_gap, "refresh budget exceeded"
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
