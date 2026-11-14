# Queue contract test pilot

Baseline: e4fc067. Only tests and documentation change. No dependencies,
production code, CI selection, subprocess modes or legacy unittest gates change.

| Scope | Before | After |
|---|---:|---:|
| Queue contract pilot | 49 | 49 |
| tests/test_qtop.py | 232 | 193 |
| tests/test_queue_nodes.py | 10 | 49 |
| Both affected modules | 242 | 242 |
| Complete pytest suite | 1539 | 1539 |
| Test source LOC, both modules including helpers | 1070 | 1099 |

Net test LOC: +29. Count nonblank, non-comment Python lines, including imports,
decorators, case tables, docstrings and the existing node helper. No helper file,
fixture framework or additional execution mode is introduced. The extra lines
make case inputs and diagnostic IDs explicit.

## Scenario mapping

Old blocking-policy tests were in tests/test_qtop.py. All new contract tests
are in tests/test_queue_nodes.py. Parameter values retain their original types,
case and order within each scenario. Each expanded row remains a pytest item.

- test_queue_is_blocked (16 cases) becomes test_queue_blocking_contract.
  The Cartesian product is unchanged: scheduler None, pbs, slurm and oar;
  states D=True, DS=True, E=False and Q=False. IDs are default-D through
  default-Q, then pbs-D through pbs-Q, slurm-D through slurm-Q, and
  oar-D through oar-Q. None is labelled default; it is still passed as None.
- test_slurm_partition_blocking_markers (7) joins that same strict contract.
  IDs are slurm-partition-up, slurm-partition-UP, slurm-partition-down,
  slurm-partition-drain, slurm-partition-inact, slurm-partition-INACTIVE
  and slurm-partition-?. Expected values remain False, False, True, True,
  True, True and False, respectively. The assertion still uses is.
- test_sge_blocking_flags_are_detected_anywhere (12) becomes
  test_sge_queue_blocking_flags[blocked-STATE] for each original state:
  a, A, c, C, d, D, E, o, s, S, u and Sdu.
- test_sge_neutral_states_are_not_marked_blocked (4) becomes the same SGE
  contract with IDs neutral-empty, neutral--, neutral-? and neutral-r.
  Both SGE groups still check truthiness, rather than boolean identity.

Old membership/configuration tests were already in tests/test_queue_nodes.py:

- test_native_membership_counts_idle_nodes_once_and_rejects_unavailable_nodes
  (1) becomes test_queue_membership_contract with ID
  slurm-native-membership-deduplicates-and-excludes-unavailable.
  All five node records, duplicate node, unavailable flag, job allocation,
  queue ordering and three expected count tuples are preserved.
- test_observed_job_queues_do_not_establish_eligibility (2) becomes that
  membership contract with IDs pbs-observed-jobs-do-not-prove-eligibility
  and oar-observed-jobs-do-not-prove-eligibility. Neither backend infers
  eligibility from the observed batch job queue.
- test_disabled_and_gpu_only_allocations_are_not_idle (1) becomes that
  membership contract with ID slurm-disabled-and-gpu-allocated-nodes-are-busy.
  The disabled node, GPU allocation and expected (batch, 0, 2) are unchanged.
- test_yaml_mapping_handles_padded_ranges_overlap_and_missing_nodes (1)
  stays focused and unchanged: same YAML bytes, parsing, count assertion
  and both sanitization assertions.
- test_invalid_membership_mappings_are_rejected (5) retains its exception
  assertion and original inputs. Old IDs True, mapping1, mapping2, mapping3
  and mapping4 become boolean, list, empty-queue-name, descending-host-range
  and nontext-host-pattern, respectively.

Scheduler-specific parsing cases in tests/plugins, PBS sample regressions and
custom-backend integration checks remain untouched. These queue contracts
call the same public qtop wrappers and queue_node_counts entry points as before.
The test-fast target continues selecting exactly the same scenarios: neither
of these two modules belongs to that target. Normal, parallel and coverage
collection still include all 49 pilot cases; web serialization is unchanged.

## Validation

Linux/WSL, CPython 3.12.16 and pytest 9.2.0:

- Before refactoring: all 49 selected pilot cases passed.
- After refactoring: all 242 cases in the affected modules passed.
- A collection audit compared the original blocking decorators with the new
  collected parameters: all 39 input/expectation/comparison-mode tuples match.
- make pre-pr passed: 1,535 parallel cases and 4 serial web cases, totaling
  1,539. Runtime coverage is 95.26% statements and 91.27% branches.
- The same gate passed 13 scheduler samples, lint, formatting, fortifications,
  mypy, Pyright, ty and both fuzzers (seed 488, 5,000 cases each).

Retained gate reports: ~/.local/state/qtop-test-reports/run-h03kh065.
No schema change was made; schema compatibility tooling was not rerun.
