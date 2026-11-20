# SonarQube review

The review dated 2026-11-14 covers 24 S8707 path findings, eight S8786
regex findings, four S8519 eager-list findings, two S2583 required-text
conditions and one S1656 self-assignment. Other rule families are outside
this review. Line numbers below refer to the scanned source.

## Changes

- Replace the four first-item list conversions in `mvu_query.py` with iterators.
- Make the legacy display model cache explicit; retain the property setter.
- Remove both unreachable SGE required-text fallbacks after validation.
- Simplify all eight reported expressions: OAR node-row parsing, PBS core/job
  masking and core selection, Slurm detail validation/masking, node-state
  suffixes and allocation headers, and trace-export account-row parsing.
- Contain scheduler command output within the configured save directory,
  including resolved parent paths and sibling-prefix collisions.
- Validate report directories and file destinations before writes. Reject
  symlink components and nonregular report destinations. Replace reports
  atomically using private temporary files, preserving hard-linked originals.

## Path finding disposition

These changes harden the output operations in 16 findings. They do not impose
an arbitrary working-directory sandbox on intentionally selected CLI paths.
The output root itself remains a caller choice. An LLM runner must enforce
its permitted roots outside qtop; canonicalization is not authorization.
No findings have been dismissed or suppressed in SonarQube.

| Scanned file and lines | Treatment |
| --- | --- |
| `tools/validate_installation.py:158,164` | Validate and atomically replace report. |
| `tools/terminal_probe.py:181` | Shared private report writer. |
| `tools/private_output.py:19` | Check directory ancestors for planted links. |
| `tools/repo_sanity.py:255,280,282` | Shared private directory and report writers. |
| `tools/validate_trace_export.py:202-209` | Shared private directory and report writers. |
| `qtop_py/qtop.py:456` | Reject trace destinations outside `savepath`. |
| `tools/check_coverage.py:30` | Review: intentional caller-selected input JSON. |
| `qtop_py/yaml_parser.py:138` | Review: intentional caller-selected YAML input. |
| `qtop_py/safety.py:190` | Review: intentional caller-selected archive input. |
| `tools/validate_trace_export.py:36,164` | Review: selected trace and reference inputs. |
| `qtop_py/qtop.py:803` | Review: intentional `-s` working-directory selection. |
| `qtop_py/trace_archive.py:36,74` | Review: selected input archive; output uses configured basenames, rejects unsafe archive members and opens exclusively in the private workspace. |

The final eight findings need review in SonarQube with these boundaries in
mind. A further policy restricting input locations would change supported
CLI behavior and needs an explicitly chosen allowed root. The next scan is
required to establish which findings SonarQube recognizes as resolved.

## Review 2026-11-20

This review covers 27 open findings: 13 S8707-style path findings, one
command-argument finding, six shell `[` tests, four regex-runtime findings, three
HTML findings and one regex-precedence finding. Line numbers refer to the
scanned source. No finding is suppressed in SonarQube; each one left as it is
has a `Sonar ... finding` comment at its line that points here.

### Changed (14)

| Scanned file and lines | Change |
| --- | --- |
| `tools/repo_sanity.py:66` | `check_diff` refuses a `--base-ref` starting with `-` (or empty) before running git. |
| `ci/scripts/matrix_lane.sh:35,42,92,114` (6) | `[[ ]]` tests; the script is bash. |
| `qtop_py/web/index.html:1,3` (3) | DOCTYPE, `lang="en"` and `<title>`. |
| `tools/repo_sanity.py` `GENERATED_OR_BINARY` | Each alternative grouped explicitly; same matches. |
| `qtop_py/plugins/slurm.py:381,383` | `\S+?=` became `\S[^\s=]*=`: the same names, checked on 200000 random strings. |

### Left as they are (14)

| Scanned file and lines | Reason |
| --- | --- |
| `qtop_py/fileutils.py:57` | The `-s` input path is the caller's choice. |
| `qtop_py/runtime.py:206` | `chdir` into the `-s` directory or savepath, both chosen by the caller. |
| `qtop_py/safety.py:190` | Caller-selected archive input, as in the 2026-11-14 review. |
| `qtop_py/trace_archive.py:36,74` | Caller-selected archive; output as in the 2026-11-14 review. |
| `tools/check_coverage.py:30` | Caller-selected coverage JSON. |
| `tools/validate_trace_export.py:37,169` | Caller-selected trace and reference inputs. |
| `qtop_py/process.py:188` | Destination is checked against the real savepath (`commonpath`) before the command runs. |
| `qtop_py/schedulers/collector.py:227` | The trace name is `os.path.basename()` inside the working directory. |
| `tools/private_output.py:26,58` | Symlinked ancestors and non-directories are rejected; replacement is atomic; the root is the caller's choice. |
| `qtop_py/plugins/pbs.py:358` | The `(?<![0-9,\-])` lookbehind lets a match start only at the beginning of a run, so matching stays linear. |
| `qtop_py/plugins/slurm.py:589` | The `(?<!\s)` lookbehind lets `\s+` start only at the beginning of a whitespace run. |

Restricting input locations would change supported CLI behaviour and needs an
explicitly chosen allowed root, as the earlier review notes. These findings
can be marked "Safe" or "Won't fix" in SonarQube with a link to this section.

## Follow-up 2026-11-20

Three findings, all changed rather than left as they are. The next scan shows
whether SonarQube recognizes the trace-archive pair as resolved; the input
archive is still the caller's choice, which no allowed-root policy restricts.

| Scanned file and lines | Change |
| --- | --- |
| `qtop_py/trace_archive.py:37` | The archive is opened once, without following a final symlink, and must be a regular file; a FIFO or device (e.g. `-s /dev/stdin`) is refused instead of read. `tarfile` reads that descriptor, not the path. |
| `qtop_py/trace_archive.py:76` | Traces are created through a descriptor of the workspace, which must be a non-symlink directory owned by the current user with no group or other access. Each configured name must be a single path component, and each file is created with `O_EXCL`, `O_NOFOLLOW` and mode 0600. No destination path is joined from CLI input. |
| `ci/baseline/Dockerfile:7` | The image ends with `USER 1001:1001`, an unprivileged `qtop` user whose uid matches the GitHub Actions runner, so the container job can still write its mounted workspace. |
