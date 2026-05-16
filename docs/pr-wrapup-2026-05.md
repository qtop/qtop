# PR wrap-up notes, 2026-05

Candidate clean-up paths for issue #357:

- #345: rebase to `develop` or close after confirming whether the small SGE
  finally/test fix is already covered.
- #334: keep closed; the redundant-import cleanup was superseded by later
  maintenance work.
- #342: keep closed unless the minerador generic API task can be mapped to a
  concrete qtop issue.
- #341: keep closed for the same minerador/generic API reason as #342.
- #306: extract the SGE multi-slot behavior into a fresh `develop` PR if still
  useful; the original branch target is stale.
- #241: close unless Python 2.6 compatibility is explicitly revived; the
  backport branch is too old for direct merge.

Suggested order: decide #345 first, keep #334/#342/#341 closed, then make a
maintainer call on whether #306 or #241 still represent active roadmap work.
