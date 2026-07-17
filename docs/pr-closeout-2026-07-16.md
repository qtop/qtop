# Challenge 357: current small-PR closeout

This documentation-only audit refreshes the backlog against `develop` on
2026-07-16. It covers seven open pull requests whose textual diffs are below
20 changed lines (GitHub's additions plus deletions; pure renames excluded).
No source code, tests, workflows, fixtures, or generated artifacts change here.

## Closeout queue

| PR | Diff | Recommendation | Current evidence |
| --- | ---: | --- | --- |
| [#429](https://github.com/qtop/qtop/pull/429) | 6 | Close | It claims docs-only challenge #357 but changes runtime code. The patch also references `__version__` before its later import, contains an invalidly indented `sys.exit(0)`, and still fails DCO. |
| [#497](https://github.com/qtop/qtop/pull/497) | 17 | Close; replace only with a focused Windows fix | Its stated Windows guard sets `termios = None`, while `raw_mode()` and other paths still dereference `termios`. Linux CI passes, but the claimed Windows behavior is neither completed nor tested. |
| [#499](https://github.com/qtop/qtop/pull/499) | 11 | Close as superseded | It targets `main`, fails DCO, and bundles four unrelated cleanups. The viewport portion is covered more precisely by current `develop` PR #526; the exception change overlaps #497 without resolving that PR's portability gap. |
| [#525](https://github.com/qtop/qtop/pull/525) | 3 | Close | It targets `main`, fails DCO, and catches every `Exception` at the CLI boundary, which can hide programming faults. Its body says it includes #400, but its one-file diff contains only the three-line catch-all. |
| [#528](https://github.com/qtop/qtop/pull/528) | 4 | Close and recreate with compatible pins on `develop` | It targets `main` and upgrades to pip 26.1.2, whose own release note in the PR says Python 3.9 support was dropped. That cannot satisfy qtop's documented Python 3.6/RHEL 8 compatibility requirement. |

## Keep in the review queue

| PR | Diff | Recommendation | Current evidence |
| --- | ---: | --- | --- |
| [#490](https://github.com/qtop/qtop/pull/490) | 4 | Keep for maintainer review | It directly addresses open issue #489 with 100%-similarity `git mv` renames, targets `develop`, is DCO-signed, and has passing checks. This is a real history-preservation decision, not backlog noise. |
| [#502](https://github.com/qtop/qtop/pull/502) | 15 | Rebase and sign off, then review | It is a maintainer-authored clarification of contribution and PoH guidance. Its CI checks pass, but DCO currently fails; preserving it as a review item is more accurate than calling it superseded. |

## Result

Closing the five items in the first table removes mis-targeted, invalid,
duplicated, or incompatible work without discarding the two small PRs that
still have distinct maintainer value. Each recommendation was checked against
the current PR diff, base branch, check status, issue linkage, and `develop`.

AI assistance disclosure: OpenAI Codex using GPT-5 (the model version exposed
to this session) assisted with repository inspection and drafting. The
contributor reviewed the evidence and remains responsible for this note.
