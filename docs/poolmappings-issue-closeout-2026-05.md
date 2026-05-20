# poolMappings issue closeout notes, 2026-05

Candidate resolution paths for Challenge #358:

- #98: re-test `-G` with a missing `user_details_cache`; close if current
  config validation logs a warning instead of raising `NoneType.split`,
  otherwise add a tiny guard around the pool-mapping detail command.
- #210: split into three decisions. The top summary already reports
  total/up/free nodes and pool mappings already include per-user node counts;
  keep only the queue-level node availability request open as a focused follow-up.
- #299: avoid per-user shelling from the display loop. If this is still wanted,
  add an optional cached group lookup beside GECOS with a fixture-backed test;
  otherwise close as a stale site-local customization.
- #303: add one PBS parser fixture for a job name containing `&` and close once
  the qstat line no longer logs an unexpected-character parse error.
- #304: re-check ANSI background mappings with a small color fixture; close if
  current output is stable, otherwise fix width accounting for background-color
  escape sequences only.

Suggested order: #304, #303, #98, #210, then #299. That path starts with
the narrowest verification tasks, keeps runtime changes reviewable, and leaves
larger feature decisions until the stale reports have fresh evidence.

Current sample-run evidence was collected on PBS, OAR, and SGE contrib data and
attached to the PR discussion rather than committed to the main repository.
