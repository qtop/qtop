# Runtime issue wrap-up candidates, May 2026

Challenge #358 asks for stale, fixed, or small older issues that can be closed or resolved without a large PR. This note focuses on runtime-oriented issues rather than community or process docs.

| Issue | Current signal | Suggested resolution path |
| --- | --- | --- |
| #303: PBS job names with special characters | PR #399 targets the concrete `&`, `$`, `{`, and hyphen examples from the thread. | Review #399 against the issue examples, require the regression fixture to include at least the reported names, then close #303 if it lands. |
| #291: `-s` points at a scheduler output file | PR #391 is a focused fix for normalizing file paths to their parent directory. | Accept #391 if the regression covers the original traceback path, otherwise close it as the preferred implementation note for a follow-up. |
| #290: missing `rem_empty_corelines` config | PR #398 adds a default for legacy or minimal config files. | Recompute mergeability for #398, keep the missing-key regression, and close #290 once the defaulting behavior lands. |
| #288: SLURM command collection | Challenge #3 produced multiple Slurm parser/plugin PRs, while #288 is only the old command-reference seed. | After one Slurm PR is selected, close #288 as superseded and keep any useful command notes in that Slurm PR's tests or docs. |
| #245: MANIFEST cleanup | Challenge #2 already caused packaging cleanup attempts and related `develop` updates. | Recheck `MANIFEST.in` after the accepted Challenge #2 changes; close #245 if stale references are gone, or extract the remaining entries into a tiny packaging-only PR. |
