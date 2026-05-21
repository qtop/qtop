# Issue #358 wrap-up candidates

This triage proposes five small, pre-2026-05-15 issues that can be wrapped up
without a large code change.

| Issue | Current signal | Suggested resolution path |
| --- | --- | --- |
| #333 | `CODE_OF_CONDUCT` is still absent from the tree. | Add a standard `CODE_OF_CONDUCT.md`, then close #333. |
| #331 | `.github/` only contains workflows, no issue/PR templates. | Add minimal issue and PR templates, then close #331. |
| #327 | Branching/release guidance is not documented in-tree. | Add a short release/branching note, then close #327. |
| #330 | Tooling discussion stalled after DeepSource comparison request. | Ask for current tool URLs or close as stale if no response. |
| #154 | OSS checklist has remaining repo hygiene items. | Split into concrete follow-ups: `.editorconfig`, package metadata, and project site. |

Notes:
- All candidates are older than 2026-05-15 and small enough for separate PRs.
- This PR intentionally does not implement the follow-ups; it keeps #358 under
  the requested 40-line limit and provides a maintainer decision path.
- AI assistance used: Hermes Codex GPT / OpenAI GPT-5.5 via Hermes Agent.
