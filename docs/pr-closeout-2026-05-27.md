# Challenge 357 PR Closeout Note

This docs-only note follows the updated #357 scope: consolidate existing pull
requests into clear closure decisions. It changes no qtop code, tests, fixtures,
workflows, screenshots, or generated artifacts.

The selection below favors pull requests that are mis-targeted, obsolete,
wrongly claiming #357, or too broad to review as-is. It avoids closing compact
current `develop` PRs unless they are provably superseded.

## Closeout Queue

| PR | Recommendation | Reason |
| --- | --- | --- |
| #429 | Close | It claims #357 but adds `-V/--version` code on `main`; the updated challenge is docs-only PR closeout work. |
| #437 | Close | #337 is already closed after the accepted debugging work, while this PR targets `main` and vendors output files. |
| #413 | Close as out of scope | It is an issue-triage note for #358/pool mappings, not a PR-backlog closeout for #357. |
| #368 | Close or ask for a fresh issue PR | It is a code fix for #308, targets `main`, and claims #357 despite the docs-only scope. |
| #351 | Close or ask for a fresh `develop` PR | It targets `main`; the useful portability idea should be retested and resubmitted against current `develop` if still wanted. |
| #306 | Close unless the author retargets | It is an old SGE change against `develop-20170714-sge`; the maintainer already asked whether the author can retarget and test it. |
| #241 | Close as obsolete | It is a Python 2.6/backport dependency change against the old SGE branch; current guidance avoids new dependencies. |
| #436 | Close and request a narrow PR | It mixes a pool-mapping change with many unrelated files and Slurm artifacts, so it is not reviewable as a small cleanup. |

## Scope Notes

This closeout deliberately avoids recommending closure for compact current
`develop` PRs such as #364, #399, #400, #420, #391, #406, and #353. Those are
normal review/change-request candidates, not #357 documentation cleanup.

AI assistance disclosure: OpenAI Codex CLI 0.134.0 with GPT-5 assisted with the
PR audit and wording. I reviewed the final note and remain responsible for it.
