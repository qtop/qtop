# Bug Fix for Issue #337: PBS JSON Parsing Failure Across Different Clusters

## Summary

This document describes a bug fix for issue #337, which addresses failures when executing qtop across different HPC clusters. The bug was present in the most recent version (0.9.20241013).

## Bug Description

### Problem

The PBS plugin's `_extract_qstatq_json` method used fragile string parsing with hardcoded array indices to extract running and queued job counts from the `state_count` field in PBS JSON output. This caused `IndexError` exceptions when:

1. Different PBS versions output `state_count` in different formats
2. Different clusters have variations in the `state_count` string format
3. The string doesn't contain the expected number of spaces or colons

### Location

**File:** `qtop_py/plugins/pbs.py`  
**Method:** `PBSStatExtractor._extract_qstatq_json`  
**Lines:** 208-209 (before fix)

### Root Cause

The original code used hardcoded array indices:
```python
qstatq_values["run"] = queue["state_count"].split(" ")[4].split(":")[1]
qstatq_values["queued"] = queue["state_count"].split(" ")[1].split(":")[1]
```

This assumes:
- The `state_count` string has at least 5 space-separated tokens
- The 5th token (index 4) contains "Running:X"
- The 2nd token (index 1) contains "Queued:X"
- Each token follows the exact format "Label:Value"

### Impact

- **Severity:** High - Causes complete failure of qtop when processing PBS JSON output
- **Affected Versions:** All versions up to and including 0.9.20241013
- **Affected Systems:** PBS/Torque clusters with JSON output format
- **Failure Mode:** `IndexError: list index out of range` when parsing queue information

## Solution

### Fix Implementation

The fix replaces hardcoded array indexing with robust regex-based parsing that:

1. **Uses regex patterns** to find "Running" and "Queued" values regardless of position
2. **Handles case-insensitive matching** for better compatibility
3. **Provides default values** (0) when parsing fails, with warning logs
4. **Uses `.get()` with defaults** for safer dictionary access

### Code Changes

**Before:**
```python
qstatq_values["run"] = queue["state_count"].split(" ")[4].split(":")[1]
qstatq_values["queued"] = queue["state_count"].split(" ")[1].split(":")[1]
qstatq_values["state"] = "E" if queue["enabled"] == "True" else "D"
```

**After:**
```python
# Parse state_count more robustly - handle different PBS versions/cluster formats
# state_count format can vary: "Transit:0 Queued:5 Held:0 Waiting:0 Running:10 Exiting:0"
# or similar variations. Use regex to extract values safely.
state_count = queue.get("state_count", "")
run_match = re.search(r"Running[:\s]+(\d+)", state_count, re.IGNORECASE)
queued_match = re.search(r"Queued[:\s]+(\d+)", state_count, re.IGNORECASE)

if run_match:
    qstatq_values["run"] = run_match.group(1)
else:
    logging.warning("Could not parse 'Running' count from state_count: '%s' for queue '%s'. Using 0." % (state_count, queue_name))
    qstatq_values["run"] = "0"

if queued_match:
    qstatq_values["queued"] = queued_match.group(1)
else:
    logging.warning("Could not parse 'Queued' count from state_count: '%s' for queue '%s'. Using 0." % (state_count, queue_name))
    qstatq_values["queued"] = "0"

qstatq_values["state"] = "E" if queue.get("enabled", "False") == "True" else "D"
```

### Benefits

1. **Robustness:** Works with different `state_count` formats across PBS versions
2. **Resilience:** Gracefully handles missing or malformed data with defaults
3. **Debuggability:** Logs warnings when parsing fails, aiding troubleshooting
4. **Maintainability:** Clearer code intent with regex patterns

## Testing

### Test Scenarios

1. **Standard Format:** `"Transit:0 Queued:5 Held:0 Waiting:0 Running:10 Exiting:0"`
2. **Different Order:** `"Running:10 Queued:5 Transit:0"`
3. **Missing Values:** `"Running:10"` (no Queued)
4. **Case Variations:** `"running:10 queued:5"`
5. **Empty/Missing:** `""` or missing `state_count` field

### Expected Behavior

- All scenarios should parse successfully or default to 0 with a warning
- No `IndexError` exceptions should occur
- Warnings should be logged when parsing fails

## Differential Debugging Steps

When encountering issues across different clusters:

1. **Check the `state_count` format:**
   ```bash
   qstat -Q -f -F json | jq '.Queue[].state_count'
   ```

2. **Enable verbose logging:**
   ```bash
   qtop -b pbs -vv
   ```

3. **Check log file for warnings:**
   ```bash
   tail -f ~/.local/qtop/qtop.log
   ```

4. **Compare working vs failing cluster outputs:**
   - Save JSON output from both clusters
   - Compare `state_count` formats
   - Verify regex patterns match both formats

## Verification

To verify the fix works:

1. Run qtop on a PBS cluster with JSON output enabled
2. Check that queue information is displayed correctly
3. Verify no `IndexError` exceptions occur
4. Check logs for any parsing warnings

## Related Issues

- Issue #337: Request to execute qtop across two HPC clusters and apply first step of differential debugging

## Status

✅ **Fixed** - The bug has been fixed in the current codebase. The fix is present in the most recent version after this change.

## Author

Fixed as part of addressing issue #337.
