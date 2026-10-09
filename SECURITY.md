# Security Policy

## Supported versions

qtop is currently pre-release software. Security fixes are made on the
`develop` branch and included in the next release. Please reproduce a report
with the latest `develop` revision when practical.

## Reporting a vulnerability

Please do not open a public issue for a vulnerability that has not yet been
disclosed. Prefer GitHub's
[private vulnerability reporting form](https://github.com/qtop/qtop/security/advisories/new),
or report it privately via email to any maintainer listed in qtop's package
metadata and source headers.

Use `qtop security report` in the subject. Include the affected qtop version
or commit, scheduler family, operating system and Python version, a minimal
reproducer, the expected impact, and any suggested mitigation. Remove cluster
names, account names, scheduler output, credentials, and other sensitive data
unless they are essential to the report. The tool qtop has an experimental
anonymization feature: use it, however review content before sending!

The maintainers will coordinate validation, remediation, release timing, and
credit with the reporter. If email is unsuitable, ask for an alternative
private channel without including vulnerability details in the initial public
request.
