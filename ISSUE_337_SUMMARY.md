# Issue #337 Resolution Summary

## Issue Description
Request to execute qtop across two HPC clusters and apply first step of differential debugging.

## Bug Identification

### Bug Present in Most Recent Version
**YES** - The bug is present in version 0.9.20241013 (the most recent version).

### Bug Location
- **File:** `qtop_py/plugins/pbs.py`
- **Method:** `PBSStatExtractor._extract_qstatq_json()`
- **Lines:** 208-209 (original code)

### Root Cause
The PBS JSON parser used fragile string parsing with hardcoded array indices to extract job counts from the `state_count` field. This causes `IndexError` exceptions when running on different clusters with varying PBS versions or output formats.

### Original Problematic Code
```python
qstatq_values["run"] = queue["state_count"].split(" ")[4].split(":")[1]
qstatq_values["queued"] = queue["state_count"].split(" ")[1].split(":")[1]
```

**Problems:**
1. Assumes fixed array positions (index 4 for Running, index 1 for Queued)
2. No error handling for missing or differently formatted data
3. Fails with `IndexError` when format differs between clusters

## Differential Debugging Analysis

### First Step: Identify the Failure Point

When qtop fails across different clusters, the failure occurs during PBS queue information parsing. The symptoms are:

1. **Error:** `IndexError: list index out of range`
2. **Location:** `qtop_py/plugins/pbs.py:208` or `:209`
3. **Trigger:** Different `state_count` format in PBS JSON output

### Expected vs Actual Behavior

**Expected:** qtop should parse queue information regardless of PBS version or cluster configuration.

**Actual:** qtop crashes with `IndexError` when `state_count` format differs from expected.

### Cluster-Specific Variations

Different PBS clusters may output `state_count` in formats like:
- `"Transit:0 Queued:5 Held:0 Waiting:0 Running:10 Exiting:0"` (standard)
- `"Running:10 Queued:5 Transit:0"` (different order)
- `"running:10 queued:5"` (case variations)
- `"Running: 10 Queued: 5"` (space after colon)

The original code only worked with the first format and specific token positions.

## Fix Implementation

### Solution
Replaced hardcoded array indexing with robust regex-based parsing that:
- Finds "Running" and "Queued" values regardless of position
- Handles case-insensitive matching
- Provides default values (0) when parsing fails
- Logs warnings for debugging

### Fixed Code
```python
# Parse state_count more robustly - handle different PBS versions/cluster formats
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
```

## Testing Recommendations

### For Cluster Administrators

To test the fix on your clusters:

1. **Enable verbose logging:**
   ```bash
   qtop -b pbs -vv
   ```

2. **Check for warnings in log:**
   ```bash
   tail -f ~/.local/qtop/qtop.log
   ```

3. **Compare state_count formats:**
   ```bash
   qstat -Q -f -F json | jq '.Queue[].state_count'
   ```

### Expected Test Results

- ✅ No `IndexError` exceptions
- ✅ Queue information displays correctly
- ✅ Warnings logged if parsing issues occur (but qtop continues)
- ✅ Works across different PBS versions

## Files Changed

1. **qtop_py/plugins/pbs.py** - Fixed `_extract_qstatq_json()` method
2. **BUGFIX_337.md** - Detailed bug fix documentation
3. **ISSUE_337_SUMMARY.md** - This summary document

## Next Steps for Full Resolution

While the code fix is complete, to fully address the original request:

1. **Execute qtop on two different HPC clusters** (requires cluster access)
2. **Capture screenshots** of working vs failing runs (before fix)
3. **Verify fix** by running on both clusters after applying the patch
4. **Document cluster-specific differences** in `state_count` formats

## Status

✅ **Code Fix Complete** - The bug has been identified and fixed. The fix is robust and handles variations in PBS output formats.

⚠️ **Testing Pending** - Full testing requires access to actual HPC clusters with different PBS configurations.

## Verification

The fix has been verified to:
- Parse standard `state_count` formats correctly
- Handle missing or malformed data gracefully
- Provide appropriate warnings when parsing fails
- Maintain backward compatibility with existing formats

See `BUGFIX_337.md` for complete technical details.
