# qtop Roadmap

This roadmap tracks near-term quality, packaging, and scheduler-support
work for qtop. It is intentionally checklist-based so follow-up issues
and pull requests can point to concrete slices instead of a stale
wishlist.

## Recently Completed

- [x] Keep 1000+ node cluster cases usable without major display hurdles.
- [x] Move qtop to Python 3 and remove Python 2 runtime support.
- [x] Add Slurm support with committed trace fixtures.
- [x] Add shared Makefile entry points for local, GitHub, and GitLab CI.
- [x] Add fast committed PBS/SGE/Slurm sample gates.
- [x] Keep a dependency-light AlmaLinux 8 / Python 3.6 compatibility lane.

## Selected Early Follow-Ups

These two roadmap items are selected for early implementation because
they are small, user-visible, and align with the current QA hardening
work:

- [ ] Improve troubleshooting and error reporting so expected CLI
      failures produce concise messages instead of raw tracebacks.
- [ ] Improve release and delivery hygiene with safe Makefile targets,
      checked package metadata, and dry-run validation before publishing.

## Open Roadmap Items

- [ ] Add LSF support after representative, approved scheduler traces or
      synthetic fixtures are available.
- [ ] Continue separating presentation from calculation so parsing,
      scheduling state, and terminal rendering can be tested more
      independently.
- [ ] Improve best-effort handling for incomplete or unusual scheduler
      input.
- [ ] Export online state in JSON format where it does not duplicate the
      existing offline export behavior.
- [ ] Provide EasyBuild packaging support for sites that deploy qtop
      through EasyBuild.
- [ ] Keep expanding scheduler sample coverage with synthetic or
      explicitly approved anonymized snapshots only.

## Contribution Notes

qtop is often used on early or bare HPC clusters where additional Python
packages, internet access, or administrator privileges may not be
available. Runtime dependencies should therefore stay minimal, and CI or
developer-only dependencies should remain pinned and isolated from the
runtime path.
