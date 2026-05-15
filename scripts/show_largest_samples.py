#!/usr/bin/env python3
"""Find the 5 largest PBS samples and show their output."""
import subprocess
import sys
import os
import json
import glob

QTOP_DIR = os.path.expanduser("~/qtop")
SAMPLES_DIR = os.path.expanduser("~/qtop-test-repo/qtop5/results")


def run_qtop_json(sample_dir):
    env = os.environ.copy()
    env["TERM"] = "xterm"
    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs", "-s", sample_dir,
        "-c", "OFF", "-O", "-E", "-1", "-2", "-3",
    ]
    subprocess.run(cmd, capture_output=True, timeout=30, env=env, cwd=QTOP_DIR)
    savepath = f"/tmp/qtop_results_{os.environ.get('USER', 'mac')}"
    json_files = sorted(glob.glob(os.path.join(savepath, "qtop_json_*.json")))
    if json_files:
        with open(json_files[-1]) as f:
            return json.load(f)
    return None


def run_qtop_text(sample_dir):
    env = os.environ.copy()
    env["TERM"] = "xterm"
    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs", "-s", sample_dir,
        "-c", "OFF", "-l",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env, cwd=QTOP_DIR)
    return result.stdout


sample_dirs = sorted(glob.glob(os.path.join(SAMPLES_DIR, "gef_*")))
print(f"Analyzing {len(sample_dirs)} samples...\n")

# Collect metrics
samples = []
for sd in sample_dirs:
    data = run_qtop_json(sd)
    if data is None:
        continue
    worker_nodes = data[0]
    total_running = data[3]
    total_queued = data[4]
    total_cores = sum(int(wn.get("np", 0)) for wn in worker_nodes)
    used_cores = sum(1 for wn in worker_nodes for _ in wn.get("core_job_map", {}))
    samples.append({
        "name": os.path.basename(sd),
        "path": sd,
        "nodes": len(worker_nodes),
        "total_cores": total_cores,
        "used_cores": used_cores,
        "running": total_running,
        "queued": total_queued,
    })

# Sort by total cores (largest first)
samples.sort(key=lambda s: s["total_cores"], reverse=True)

print("=" * 80)
print("TOP 5 LARGEST PBS SAMPLES (by total cores)")
print("=" * 80)
for i, s in enumerate(samples[:5]):
    print(f"\n--- #{i+1}: {s['name']} ---")
    print(f"  Nodes:      {s['nodes']}")
    print(f"  Total cores: {s['total_cores']}")
    print(f"  Used cores:  {s['used_cores']}")
    print(f"  Running:     {s['running']}")
    print(f"  Queued:      {s['queued']}")
    print(f"  Utilization: {s['used_cores']/s['total_cores']*100:.1f}%")

    # Show text output
    text = run_qtop_text(s['path'])
    if isinstance(text, bytes):
        text = text.decode('utf-8', errors='replace')
    # Extract just the summary lines
    for line in text.split('\n')[:10]:
        print(f"  | {line.strip()}")
    print()

print("=" * 80)
print("ALL 5 BUG FIXES")
print("=" * 80)

bugs = [
    {
        "id": 1,
        "file": "qtop_py/plugins/pbs.py",
        "function": "_extract_qstatq_regex",
        "title": "UnboundLocalError when qstat -Q output lacks summary line",
        "description": "If qstat -Q output does not contain a summary line matching the "
                       "run_qd_search regex (e.g. truncated or unusual output), "
                       "total_running_jobs and total_queued_jobs variables are never "
                       "assigned, causing UnboundLocalError at line 194.",
        "fix": "Initialize total_running_jobs, total_queued_jobs = 0, 0 before the loop.",
    },
    {
        "id": 2,
        "file": "qtop_py/plugins/pbs.py",
        "function": "_extract_qstat_regex",
        "title": "UnboundLocalError in finally block when both regexes fail",
        "description": "If both user_q_search and user_q_search_prior regexes fail to "
                       "parse the first qstat line, qstat_values is never assigned, but "
                       "the finally block unconditionally references it.",
        "fix": "Remove the finally block; append after both try/except branches.",
    },
    {
        "id": 3,
        "file": "qtop_py/qtop.py",
        "function": "compress_colored_line",
        "title": "IndexError when processing empty or plain-text strings",
        "description": "When s is empty string or contains no ANSI color codes, "
                       "t[0][:-1] incorrectly strips the last character, or IndexError "
                       "is raised if t is empty.",
        "fix": "Return s immediately if t is empty after splitting.",
    },
    {
        "id": 4,
        "file": "qtop_py/plugins/pbs.py",
        "function": "_get_jobs_cores",
        "title": "UnboundLocalError for unrecognized job/core format in pbsnodes output",
        "description": "If a line in pbsnodes 'jobs' field doesn't match either Torque "
                       "or PBS Pro format, core and job are never assigned, causing "
                       "UnboundLocalError.",
        "fix": "Add 'else: continue' to skip unrecognized formats gracefully.",
    },
    {
        "id": 5,
        "file": "qtop_py/serialiser.py",
        "function": "ensure_worker_nodes_have_qnames",
        "title": "None values leaked into worker node qname set",
        "description": "When a job_id from core_job_map is not found in job_ids_queues "
                       "(e.g. stale job references), dict.get() returns None which gets "
                       "added to the my_queues set, contaminating downstream display.",
        "fix": "Filter out None values from the set comprehension.",
    },
]

for b in bugs:
    print(f"\n--- Bug #{b['id']}: {b['title']} ---")
    print(f"  File:     {b['file']}")
    print(f"  Function: {b['function']}")
    print(f"  Problem:  {b['description']}")
    print(f"  Fix:      {b['fix']}")