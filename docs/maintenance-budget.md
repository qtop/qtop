# Maintenance implementation budget

Net source LOC includes tests and docstrings, excludes blank/comment lines and
prose documentation. Moves count by their aggregate delta. Each item is checked
against its agreed upper bound before commit. No runtime dependency is added.

| Item | Net source LOC | Ceiling | Change |
|---|---:|---:|---|
| 1 | 12 | 35 | CI: isolate packaging builds per worker |
| 9 | 7 | 20 | CI: make validation defaults single-owned |
| 8 | 25 | 40 | CI: emit verified release artifact evidence |
| 7 | 82 | 100 | Release: add a dry-run release preparation command |
| 3 | 16 | 60 | Bugfix: centralize YAML container transitions |
| 4 | 6 | 30 | Bugfix: extract pure runtime input parsing |
