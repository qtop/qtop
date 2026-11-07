# Maintenance and ownership

## Maintainers and review contacts

| Role | Maintainer | Contact |
|---|---|---|
| Primary maintainer | Fotis Georgatos | [@fgeorgatos](https://github.com/fgeorgatos), [email](mailto:kefalonia@gmail.com) |
| Second maintainer | Evangelos Papadopoulos | [@evanspap](https://github.com/evanspap), [email](mailto:evangelos.papadopoulos@gmail.com) |

Maintainers function as standing backup for each other,
for review assignment, coordination and escalation. Both share maintenance and
review responsibilities. [CODEOWNERS](.github/CODEOWNERS) routes all paths
to both maintainers. Package author credits in [pyproject.toml](pyproject.toml)
are not a list of current administrators or release publishers.

## Four-eyes principle

Every change and release requires two people: the author or preparer and an
independent human reviewer. We opt that authors must not approve their own work.
When one maintainer authors a change, the other reviews it before merge.
Contributions from other authors require review by at least one maintainer.
This is also why we may not accepts PRs where the introducer may have no understanding of the code:
- that would violate the 4-eyes principle or in any case by a route to bypass it.

The reviewer checks scope, correctness, regression evidence, Python
compatibility and contribution requirements. CI, packaging and security-policy
changes receive the same independent review. Automated checks and AI assistance
support review but do not replace the second person's approval.
We are in peace with AI, aiming to use and interface with it right.

Record approval on the PR before merge. For releases, a second maintainer
verifies the exact revision, version, tag, validation results and artifact
hashes and records approval in the release-tracking issue or PR before
publication. The authorized publisher publishes those approved artifacts.
These review and release records provide evidence that the four-eyes principle
has been followed. If independent review is unavailable, escalate to the other
maintainer and wait for review.

## Responsibilities

| Work | Accountable role and handoff |
|---|---|
| Issue triage and review assignment | Default review contact or standing backup; record the assigned maintainer in the issue or PR. |
| Change review | An independent maintainer checks scope, regression evidence, compatibility and contribution requirements; an authorized maintainer merges after approval. |
| CI, packaging and security-policy changes | Either maintainer coordinates independent review with the people who hold the relevant service permissions. |
| Security reports | Default review contact or standing backup coordinates private triage and disclosure through [SECURITY.md](SECURITY.md). |
| Release preparation | Name a release coordinator in the release-tracking issue or PR; the coordinator records the revision, version, validation evidence and remaining limitations. |
| Publication | A maintainer with package-publishing authority publishes the exact independently approved artifacts. Coordination alone does not grant that access. |

Contributors follow [CONTRIBUTING.md](CONTRIBUTING.md). Review and release
decisions should be recorded on the associated issue/PR, including who owns
unfinished work. Use the private channel for undisclosed security details.

## Escalation and handover

For a stalled ordinary issue or PR, request follow-up from the default review
contact on that thread, including the blocked decision and available evidence.
Record any temporary handover on the issue or PR. Security escalation
stays private as described in SECURITY.md. No response-time SLA or unattended
merge/publication is promised.

Adding or replacing an owner requires a maintainer-reviewed PR updating this
file and CODEOWNERS together. Confirm the person's agreement and repository
write access before relying on automatic review routing. Record the scope of
any delegated role and verify publisher/environment access separately.

CODEOWNERS is routing configuration, not an access grant or a claim that
owner approval is enforced. An administrator must configure required review
approvals and prevent authors from bypassing independent review on the relevant
branches. See
[GitHub's CODEOWNERS requirements](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners).

## Release and support handoff

The release coordinator checks version/tag/release-note consistency, links
the required CI and compatibility evidence, and identifies the independent
reviewer and authorized publisher before publication. After publication,
record the release URL and an installation check for the published package.
Keep artifacts in CI or `qtop-artifacts`, following CONTRIBUTING.md.

Security fixes target `develop`; release and backport support boundaries are
defined in [SECURITY.md](SECURITY.md#supported-versions). Any exception must name
the affected version/branch, responsible maintainer and support end condition
in the release record, with a corresponding SECURITY.md update.
