# Getting help with qtop

qtop is maintained by volunteers ([MAINTAINERS.md](MAINTAINERS.md)). Support is
best effort, through public GitHub issues; there is no paid or guaranteed
response time.

## Before asking

- Read the [quickstart](docs/quickstart.md), the
  [user documentation](docs/documentation.md) and `qtop --help` / `man qtop`.
- Try the built-in demo, `./qtop -b demo -FGTw`, to tell an installation
  problem from a scheduler one.
- Check [open and closed issues](https://github.com/qtop/qtop/issues?q=is%3Aissue)
  for the same symptom.

## Asking for help or reporting a bug

Open an issue with the [bug report form](https://github.com/qtop/qtop/issues/new/choose).
Include:

- `qtop --version` (or the commit), the scheduler and its version, the
  operating system and the Python version;
- the exact command line and what you expected instead;
- if you can, an anonymized support archive, `qtop -L -A`, which another
  person can replay with `qtop --load-sample archive.tar` without your
  scheduler. Review it before attaching it; see the
  [anonymization scope](docs/documentation.md#anonymization-scope).

Questions, ideas and feature requests are welcome as issues too.

## What is supported

- **Runtime:** CPython 3.6 or newer on Linux, with no third-party dependencies.
- **Schedulers:** PBS/Torque and PBS Pro, Slurm, SGE and OAR, plus the demo.
- **Versions:** fixes land on `develop` and ship with the next release; see the
  supported-versions table in [SECURITY.md](SECURITY.md).

## Security issues

Do not open a public issue for an undisclosed vulnerability. Follow
[SECURITY.md](SECURITY.md) to report it privately.

## Contributing

Patches, scheduler samples and documentation fixes are welcome: see
[CONTRIBUTING.md](CONTRIBUTING.md) and the [code of conduct](CODE_OF_CONDUCT.md).
