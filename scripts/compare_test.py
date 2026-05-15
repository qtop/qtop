#!/usr/bin/env python3
"""
Full batch test: run qtop on all 447 PBS samples and compare with expected output.
"""
import subprocess
import sys
import os
import json
import glob
import re
import tempfile

QTOP_DIR = os.path.expanduser("~/qtop")
SAMPLES_DIR = os.path.expanduser("~/qtop-test-repo/qtop5/results")

def run_qtop_text(sample_dir):
    """Run qtop on a sample directory and return the text output."""
    env = os.environ.copy()
    env["TERM"] = "xterm"
    env["TERMINFO"] = "/usr/share/terminfo"
    
    cmd = [
        sys.executable, "-m", "qtop_py.cli",
        "-b", "pbs",
        "-s", sample_dir,
        "-c", "OFF",
        "-l",  # Allow overflow
        "-1", "-2", "-3",  # Disable sections to speed up
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
        cwd=QTOP_DIR,
    )
    
    return result.stdout, result.stderr

def compare_with_expected(sample_dir):
    """Compare qtop output with expected qtop.out."""
    name = os.path.basename(sample_dir)
    expected_file = os.path.join(sample_dir, "qtop.out")
    
    if not os.path.exists(expected_file):
        return {"name": name, "status": "NO_EXPECTED"}
    
    with open(expected_file) as f:
        expected = f.read()
    
    try:
        actual, stderr = run_qtop_text(sample_dir)
    except subprocess.TimeoutExpired:
        return {"name": name, "status": "TIMEOUT"}
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}
    
    # Compare key metrics
    # Extract "X/Y Nodes" from both
    def extract_nodes(text):
        m = re.search(r'(\d+)/(\d+)\s*Nodes', text)
        if m:
            return int(m.group(1)), int(m.group(2))
        return None, None
    
    def extract_cores(text):
        m = re.search(r'(\d+)/(\s*\d+)\s*Cores', text)
        if m:
            return int(m.group(1)), int(m.group(2).strip())
        return None, None
    
    def extract_jobs(text):
        m = re.search(r'(\d+)\+(\d+)\s*jobs', text)
        if m:
            return int(m.group(1)), int(m.group(2))
        return None, None
    
    exp_nodes = extract_nodes(expected)
    act_nodes = extract_nodes(actual)
    exp_cores = extract_cores(expected)
    act_cores = extract_cores(actual)
    exp_jobs = extract_jobs(expected)
    act_jobs = extract_jobs(actual)
    
    issues = []
    if exp_nodes != act_nodes:
        issues.append(f"Nodes: expected {exp_nodes}, got {act_nodes}")
    if exp_cores != act_cores:
        issues.append(f"Cores: expected {exp_cores}, got {act_cores}")
    if exp_jobs != act_jobs:
        issues.append(f"Jobs: expected {exp_jobs}, got {act_jobs}")
    
    return {
        "name": name,
        "status": "ISSUES" if issues else "MATCH",
        "issues": issues,
        "expected_nodes": exp_nodes,
        "actual_nodes": act_nodes,
        "expected_cores": exp_cores,
        "actual_cores": act_cores,
        "expected_jobs": exp_jobs,
        "actual_jobs": act_jobs,
    }

def main():
    sample_dirs = sorted(glob.glob(os.path.join(SAMPLES_DIR, "gef_*")))
    print(f"Found {len(sample_dirs)} samples")
    
    results = []
    for i, sd in enumerate(sample_dirs):
        result = compare_with_expected(sd)
        results.append(result)
        
        if result["status"] == "MATCH":
            print(f"[{i+1}/{len(sample_dirs)}] ✓ {result['name']}: MATCH")
        elif result["status"] == "ISSUES":
            print(f"[{i+1}/{len(sample_dirs)}] ⚠ {result['name']}: ISSUES")
            for issue in result["issues"]:
                print(f"       - {issue}")
        else:
            print(f"[{i+1}/{len(sample_dirs)}] ✗ {result['name']}: {result['status']}")
    
    # Summary
    match = sum(1 for r in results if r["status"] == "MATCH")
    issues = sum(1 for r in results if r["status"] == "ISSUES")
    no_exp = sum(1 for r in results if r["status"] == "NO_EXPECTED")
    errors = sum(1 for r in results if r["status"] in ("ERROR", "TIMEOUT"))
    
    print(f"\n{'='*60}")
    print(f"Summary: {match} MATCH, {issues} ISSUES, {no_exp} NO_EXPECTED, {errors} ERRORS out of {len(results)}")
    
    # Print all issues
    if issues > 0:
        print(f"\n{'='*60}")
        print("DETAILED ISSUES:")
        for r in results:
            if r["status"] == "ISSUES":
                print(f"\n{r['name']}:")
                for issue in r["issues"]:
                    print(f"  - {issue}")

if __name__ == "__main__":
    main()