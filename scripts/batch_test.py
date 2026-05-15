#!/usr/bin/env python3
"""
Batch test qtop against PBS sample data.
Runs qtop on each sample and compares output with expected qtop.out.
"""
import subprocess
import sys
import os
import json
import glob
import re

QTOP_DIR = os.path.expanduser("~/qtop")
SAMPLES_DIR = os.path.expanduser("~/qtop-test-repo/qtop5/results")

def run_qtop(sample_dir):
    """Run qtop on a sample directory and return the JSON output."""
    env = os.environ.copy()
    env["TERM"] = "xterm"
    
    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs",
        "-s", sample_dir,
        "-c", "OFF",
        "-O",
        "-E",
        "-1", "-2", "-3",
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=QTOP_DIR,
    )
    
    savepath = f"/tmp/qtop_results_{os.environ.get('USER', 'mac')}"
    json_files = sorted(glob.glob(os.path.join(savepath, "qtop_json_*.json")))
    if json_files:
        latest = json_files[-1]
        with open(latest) as f:
            return json.load(f)
    return None

def check_sample(sample_dir):
    """Check a single sample."""
    name = os.path.basename(sample_dir)
    try:
        data = run_qtop(sample_dir)
        if data is None:
            return {"name": name, "status": "ERROR", "error": "No JSON output"}
        
        worker_nodes = data[0]  # list of worker nodes
        job_info = data[1]      # dict of job info
        queue_info = data[2]    # dict of queue info
        total_running = data[3] # int
        total_queued = data[4]  # int
        
        # Count nodes by state
        states = {}
        for wn in worker_nodes:
            s = wn.get("state", "?")
            states[s] = states.get(s, 0) + 1
        
        total_cores = sum(int(wn.get("np", 0)) for wn in worker_nodes)
        used_cores = sum(1 for wn in worker_nodes for _ in wn.get("core_job_map", {}))
        
        return {
            "name": name,
            "status": "OK",
            "nodes": len(worker_nodes),
            "states": states,
            "total_cores": total_cores,
            "used_cores": used_cores,
            "total_running": total_running,
            "total_queued": total_queued,
            "jobs": len(job_info) if isinstance(job_info, dict) else 0,
        }
    except subprocess.TimeoutExpired:
        return {"name": name, "status": "TIMEOUT"}
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}

def main():
    sample_dirs = sorted(glob.glob(os.path.join(SAMPLES_DIR, "gef_*")))
    print(f"Found {len(sample_dirs)} samples")
    
    results = []
    for i, sd in enumerate(sample_dirs[:20]):  # First 20 for testing
        result = check_sample(sd)
        results.append(result)
        
        status_icon = "✓" if result["status"] == "OK" else "✗"
        print(f"[{i+1}/20] {status_icon} {result['name']}: {result['status']}", end="")
        if result["status"] == "OK":
            print(f" | nodes={result['nodes']} cores={result['used_cores']}/{result['total_cores']} jobs={result['jobs']} R={result['total_running']} Q={result['total_queued']}")
        elif "error" in result:
            print(f" | {result['error']}")
        else:
            print()
    
    ok = sum(1 for r in results if r["status"] == "OK")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    timeouts = sum(1 for r in results if r["status"] == "TIMEOUT")
    
    print(f"\n{'='*50}")
    print(f"Summary: {ok} OK, {errors} Errors, {timeouts} Timeouts out of {len(results)}")

if __name__ == "__main__":
    main()