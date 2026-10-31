# Maintenance and ownership

## Review contact

The default review contact is **Fotis Georgatos
([@fgeorgatos](https://github.com/fgeorgatos))**, an existing qtop maintainer
([example maintainer action](https://github.com/qtop/qtop/pull/511)).
[CODEOWNERS](.github/CODEOWNERS) routes all paths to this contact until
additional area owners are explicitly recorded. Package author credits in
[pyproject.toml](pyproject.toml) are not a list of current administrators or
release publishers.

## Responsibilities

| Work | Accountable role and handoff |
|---|---|
| Issue triage and review assignment | Default review contact; record an alternate maintainer in the issue or PR when delegating. |
| Change review | Assigned reviewer checks scope, regression evidence, compatibility and contribution requirements; an authorized maintainer decides whether to merge. |
| CI, packaging and security-policy changes | Default review contact coordinates review with maintainers who hold the relevant service permissions. |
| Security reports | Default review contact coordinates private triage and disclosure through [SECURITY.md](SECURITY.md). |
| Release preparation | Name a release coordinator in the release-tracking issue or PR; the coordinator records the revision, version, validation evidence and remaining limitations. |
| Publication | A maintainer with repository and package-publishing authority verifies the release record and performs or authorizes publication. Coordination alone does not grant that access. |

Contributors follow [CONTRIBUTING.md](CONTRIBUTING.md). Review and release
decisions should be recorded on the associated issue/PR, including who owns
unfinished work. Use the private channel for undisclosed security details.

## Escalation and handover

For a stalled ordinary issue or PR, request follow-up from the default review
contact on that thread, including the blocked decision and available evidence.
If that contact is unavailable, use an existing maintainer contact published
in package metadata; no additional person is designated as a standing backup
by this document. Security escalation stays private as described in SECURITY.md.
No response-time SLA or unattended merge/publication is promised.

Adding or replacing an owner requires a maintainer-reviewed PR updating this
file and CODEOWNERS together. Confirm the person's agreement and repository
write access before relying on automatic review routing. Record the scope of
any delegated role and verify publisher/environment access separately.

CODEOWNERS is routing configuration, not an access grant or a claim that
owner approval is enforced. An administrator must configure any required
approval rule on the relevant branches. See
[GitHub's CODEOWNERS requirements](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners).

## Release and support handoff

The release coordinator checks version/tag/release-note consistency, links
the required CI and compatibility evidence, and identifies the authorized
publisher before publication. After publication, record the release URL and
an installation check for the published package. Keep artifacts in CI or
`qtop-artifacts`, following CONTRIBUTING.md.

Security fixes target `develop`; release and backport support boundaries are
defined in [SECURITY.md](SECURITY.md#supported-versions). Any exception must name
the affected version/branch, responsible maintainer and support end condition
in the release record, with a corresponding SECURITY.md update.
