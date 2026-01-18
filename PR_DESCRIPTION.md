# Fix PBS JSON parsing failure across different clusters (Issue #337)

## Summary

Fixes a critical bug in PBS JSON parsing that causes `IndexError` exceptions when running qtop across different HPC clusters with varying PBS output formats.

## Problem

The `_extract_qstatq_json` method in `qtop_py/plugins/pbs.py` used fragile string parsing with hardcoded array indices to extract job counts from the `state_count` field. This fails when:

- Different PBS versions output `state_count` in different formats
- Different clusters have variations in the `state_count` string format
- The string doesn't contain the expected number of spaces or colons

**Error:** `IndexError: list index out of range` at lines 208-209

## Solution

Replaced hardcoded array indexing with robust regex-based parsing that:
- Finds "Running" and "Queued" values regardless of position in the string
- Handles case-insensitive matching for better compatibility
- Provides default values (0) when parsing fails, with warning logs
- Uses `.get()` with defaults for safer dictionary access

## Changes

- **qtop_py/plugins/pbs.py**: Fixed `_extract_qstatq_json()` method to use regex parsing instead of hardcoded indices
- **BUGFIX_337.md**: Comprehensive technical documentation of the bug and fix
- **ISSUE_337_SUMMARY.md**: Summary addressing the original issue request

## Testing

The fix has been verified to:
- ✅ Parse standard `state_count` formats correctly
- ✅ Handle missing or malformed data gracefully
- ✅ Provide appropriate warnings when parsing fails
- ✅ Maintain backward compatibility with existing formats

## Impact

- **Severity:** High - Prevents complete failure of qtop when processing PBS JSON output
- **Affected Versions:** All versions up to and including 0.9.20241013
- **Affected Systems:** PBS/Torque clusters with JSON output format

## Related Issues

Closes #337

## Checklist

- [x] Code follows the project's style guidelines
- [x] Self-review of code completed
- [x] Comments added for complex logic
- [x] Documentation updated
- [x] No new warnings generated
- [x] Tests pass (where applicable)
- [x] Changes are backward compatible
