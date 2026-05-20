# Open PR wrap-up candidates

Small maintainer triage set for Challenge #4 (#357):

- #241: Python 2.6 `backport_collections` support is conflicting and likely obsolete after the Python-2 cleanup work in Challenge #2.
- #336: the WIP Slurm scaffold is superseded by the current Slurm challenge PR set (#360, #361, #367, #375, #380, #387, #396).
- #339: PBS JSON parsing for #337 overlaps with #344; choose one Issue #337 path, then close the other as duplicate.
- #353: legacy PBS qstat parsing is conflicting; rebase or extract only the sample cases not already covered by #349/#399.
- #306: old SGE multi-slot support is still mergeable but should be rechecked against current fixtures before review priority.
