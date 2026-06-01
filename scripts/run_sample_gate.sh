#!/bin/sh
set -eu

ARTIFACT_DIR="${ARTIFACT_DIR:-artifacts/sample-gate}"
MAX_FAILURES="${MAX_FAILURES:-0}"
PYTHON="${PYTHON:-python3}"
VOLATILE_PATTERN='WORKDIR\|Please try it with watch\|Log file created in'

failures=0

mkdir -p "$ARTIFACT_DIR"

run_case() {
    scheduler="$1"
    reference="$2"
    shift 2

    output="$ARTIFACT_DIR/qtop-$scheduler.out"
    stderr="$ARTIFACT_DIR/qtop-$scheduler.err"
    filtered="$ARTIFACT_DIR/qtop-$scheduler.filtered.out"
    expected="$ARTIFACT_DIR/qtop-$scheduler.expected.out"
    diff_file="$ARTIFACT_DIR/qtop-$scheduler.diff"

    echo "Running qtop sample gate for $scheduler"
    if ! "$PYTHON" -m qtop_py.cli "$@" > "$output" 2> "$stderr"; then
        failures=$((failures + 1))
        echo "qtop execution failed for $scheduler; see $output and $stderr"
    fi

    if ! grep -q "Job accounting summary" "$output"; then
        failures=$((failures + 1))
        echo "qtop output for $scheduler is missing the accounting summary"
    fi

    if ! grep -q "Worker Nodes occupancy" "$output"; then
        failures=$((failures + 1))
        echo "qtop output for $scheduler is missing worker node occupancy"
    fi

    grep -v "$VOLATILE_PATTERN" "$output" > "$filtered" || true
    grep -v "$VOLATILE_PATTERN" "$reference" > "$expected" || true

    if diff -u "$expected" "$filtered" > "$diff_file"; then
        rm -f "$diff_file"
    else
        echo "qtop output differs from the historical $scheduler reference; see $diff_file"
    fi

    if [ "$failures" -gt "$MAX_FAILURES" ]; then
        echo "Sample gate failed with $failures failure(s), max allowed is $MAX_FAILURES"
        exit 1
    fi
}

run_case sge qtop_py/contrib/sger_dvv_out.ref -s qtop_py/contrib -c ON -Fadvv -b sge
run_case oar qtop_py/contrib/oar1_dvv_out.ref -s qtop_py/contrib -c ON -FAardvvv -b oar
run_case pbs qtop_py/contrib/pbs_dvv_out.ref -s qtop_py/contrib -c ON -raF -b pbs

echo "Sample gate passed; artifacts written to $ARTIFACT_DIR"
