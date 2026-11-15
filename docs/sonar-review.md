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
