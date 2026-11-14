# Maintenance implementation budget

Net source LOC includes tests and docstrings, excludes blank/comment lines and
prose documentation. Moves count by their aggregate delta. Each item is checked
against its agreed upper bound before commit. No runtime dependency is added.

| Item | Net source LOC | Ceiling | Change |
|---|---:|---:|---|
| 1 | 12 | 35 | CI: isolate packaging builds per worker |
