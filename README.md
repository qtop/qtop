# qtop

[![build-qtop](https://github.com/qtop/qtop/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/qtop/qtop/actions/workflows/build.yml)
![Python 3](https://img.shields.io/badge/python-3.x-blue.svg)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/qtop/qtop/badge)](https://scorecard.dev/viewer/?uri=github.com/qtop/qtop)

qtop is a scheduler-independent observability layer for HPC clusters, with an extremely fast terminal interface. It is a portable, dependency-light cluster observability and scheduler debugging tool for Slurm, PBS, SGE and OAR - and even more, with your contributed plugin effort.

See your whole cluster in one terminal. Capture it. Replay it. Compare it.

![qtop terminal demo](https://raw.githubusercontent.com/qtop/qtop/master/qtop_py/contrib/qtop_demo.gif)

## Why qtop?

Schedulers expose a great deal of information, but their native commands rarely provide one concise view of jobs, queues, nodes, states, and core occupancy. qtop turns scheduler output into a consistent terminal dashboard that is useful during daily operations and incident analysis.

- See cluster utilization, queues, users, node states, and per-core job placement.
- Use the same interface across Slurm, PBS/Torque, SGE, and OAR.
- Filter, sort, highlight, transpose, and navigate large worker-node matrices interactively.
- Capture scheduler output for bug reports or offline investigation.
- Replay recent frames to understand scheduling changes over time.
- Export normalized cluster data as JSON.
- Run without a resident service, database, or heavy runtime dependency stack.

## Quick start

Clone the repository and launch the built-in demo:

```console
git clone https://github.com/qtop/qtop.git
cd qtop
./qtop -b demo -FGTw
```

On a scheduler host, qtop can normally discover the available scheduler:

```console
./qtop -w
```

Select it explicitly when needed:

```console
./qtop -b slurm -w
./qtop -b pbs -w
./qtop -b sge -w
./qtop -b oar -w
```

The optional value after `-w` is the refresh interval in seconds. For example, `-w 10` refreshes every ten seconds; without a value, watch mode refreshes every two seconds.

Run `./qtop --help` for the complete command-line reference. While watch mode is active, press `?` for the interactive key map.

## Installation

### From source

```console
git clone https://github.com/qtop/qtop.git
cd qtop
./qtop --version
```

### From PyPI

```console
## python3 -m pip install --user qtop ## FIXME, 20261001
$HOME/.local/bin/qtop --version
```

A system-wide or virtual-environment installation can omit `--user`.

## Capture, inspect, and replay

Read scheduler traces from a directory instead of invoking live commands:

```console
./qtop -b slurm -s /path/to/slurm-traces
```

Create a support sample containing qtop output, logs, and scheduler traces:

```console
./qtop -L
```

Replay automatically captured frames from a specific time:

```console
./qtop -R 1823
```

Export the normalized cluster state as JSON:

```console
./qtop -E
```

See [the full documentation](docs/documentation.rst) for trace filenames, configuration, filtering, replay formats, anonymization, and scheduler-specific notes.

## How it works

qtop separates scheduler-specific collection and parsing from a common cluster-state model and terminal presentation:

1. A scheduler plugin collects live command output or reads saved traces.
2. The plugin normalizes jobs, nodes, and queues into a shared cluster state.
3. qtop renders that state in a fast terminal view or exports it for other tools.

This design keeps the core scheduler-independent and makes support for another batch system a bounded plugin contribution.

## Compatibility

qtop targets Python 3 and Linux-based HPC environments. Its CI matrix covers modern Python versions and retains a dependency-light compatibility lane for older enterprise distributions commonly found on clusters.

The project is approaching 1.0 and still contains explicitly marked experimental features. Stable terminal monitoring remains the primary interface.

## Documentation and community

- [User documentation and tutorial](docs/documentation.rst)
- [Contributing guide](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Issue tracker](https://github.com/qtop/qtop/issues)

Bug reports with captured scheduler samples are especially valuable. New scheduler plugins, sample fixtures, documentation improvements, and portability fixes are welcome.

Python port by Sotiris Fragkiskos. Original Bash version by Fotis Georgatos.

License: [MIT](LICENSE).
