# Maintenance implementation budget

Net source LOC includes tests and docstrings, excludes blank/comment lines and
prose documentation. Moves count by their aggregate delta. Each item is checked
against its agreed upper bound before commit. No runtime dependency is added.

| Item | Net source LOC | Ceiling | Change |
|---|---:|---:|---|
| 1 | 12 | 35 | Packaging build isolation |
| 2 | 21 | 100 | Typed collection settings |
| 3 | 16 | 60 | YAML container transitions |
| 4 | 6 | 30 | Runtime input helpers |
| 5 | 23 | 100 | Shared Python/CUE fixtures |
| 6 | 0 | 0 | Compatible legacy retirement plan |
| 7 | 99 | 100 | Release preparation preview/write |
| 8 | 39 | 40 | Verified release evidence |
| 9 | 7 | 20 | Single-owned validation defaults |
| 10 | 56 | 70 | Nightly matrix parity |

Total: 279 net source LOC. Figures include subsequent validation corrections.
Legacy adapter deletion is reserved for 1.0; this batch adds no source lines
to its retirement plan and preserves the 0.9 public API.
