# Contributing to qtop

Thank you for considering contributing to qtop! We welcome contributions of all kinds.

## Quick Start

```bash
git clone https://github.com/qtop/qtop.git
cd qtop
pip install -e .
make test
```

## Development Workflow

1. Fork the repository on GitHub
2. Create a branch for your changes: `git checkout -b my-feature`
3. Make your changes
4. Run the tests: `make test`
5. Run lint checks: `make lint`
6. Run fortifications check: `make fortifications`
7. Commit your changes with a descriptive message
8. Push and submit a Pull Request

## Code Style

- Follow PEP 8 conventions
- Run `make format-fix` to auto-format code
- Avoid bare `eval()` calls; use `ast.literal_eval()` or `_safe_lambda_eval()` instead
- Keep line length under 188 characters

## Pull Request Guidelines

- Reference any related issues in the PR description
- Ensure all CI checks pass (lint, test, fortifications)
- Provide a screenshot or log output for visual/functional changes
- Sign your commits with a DCO (Developer Certificate of Origin)

## Reporting Issues

When reporting bugs, include:
- qtop version (`./qtop --version`)
- Your batch system (Torque/PBS/SGE/OAR)
- Steps to reproduce
- Expected vs actual behavior

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

[1] https://wiki.linuxfoundation.org/dco
[2] https://developercertificate.org/
[3] https://www.conventionalcommits.org/en/v1.0.0/
