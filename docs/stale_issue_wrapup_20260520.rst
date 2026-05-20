20260520: stale issue wrap-up candidates
========================================

This note supports challenge #358 with a small, documentation-only triage pass.
It proposes closure or follow-up paths for five pre-2026-05-15 issues without
changing runtime behavior.

* #333, ``Include CODE_OF_CONDUCT file``: still valid. Add a standard
  ``CODE_OF_CONDUCT.md`` in a dedicated PR, then close #333.
* #331, ``Issue and Pull Request Templates for Enhanced Collaboration``: still
  valid. Add minimal GitHub templates in a dedicated PR; keep this separate
  from code changes to avoid review noise.
* #308, ``OSError: [Errno 2] No such file or directory``: stale runtime report
  from 2018. Ask for a current reproducer on main; close if none is provided.
* #250, ``Understand why automated PKG-INFO upload fails``: obsolete packaging
  concern. Reframe as a modern release-automation issue if still desired;
  otherwise close as superseded by current Python packaging workflows.
* #245, ``cleanup MANIFEST.in``: still small and actionable. Handle in a tiny
  packaging-only PR after checking the source distribution contents.

Validation
----------

* Documentation-only change; no Python runtime path changed.
* Checked against CONTRIBUTING.md: AI assistance disclosed, no artifacts added.
* LLM used: OpenAI Codex GPT-5.5 via OpenClaw.
