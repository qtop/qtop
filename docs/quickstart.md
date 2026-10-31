# Quickstart

qtop shows cluster jobs, queues, nodes and CPU occupancy in a terminal.
Use a Linux shell with Python 3.6 or newer. Running from a source checkout
requires no extra Python packages or administrator privileges. By design.

## Try the demo

```sh
git clone https://github.com/qtop/qtop.git
cd qtop
./qtop --version
./qtop -b demo -w
```

The demo needs no scheduler connection. Press `?` for interactive help and
`q` to quit. Omit `-w` to print a single snapshot.

All commands below run from the checkout directory. If you installed qtop,
use `qtop` in place of `./qtop`.

## Monitor your cluster

Run on a host where the scheduler's commands are available, typically a
cluster login node. Load your site's scheduler environment/module if needed.
Let qtop detect the scheduler:

```sh
./qtop -w
```

Or select one explicitly:

```sh
./qtop -b slurm -w 10
```

Replace `slurm` with `pbs`, `sge` or `oar` for your cluster. `-w 10` refreshes
every ten seconds; `-w` alone refreshes every two seconds. Add
`--expand-queues` to show each queue or partition on its own line.

## Inspect saved scheduler output

Try the committed Slurm fixture without contacting a scheduler:

```sh
./qtop -b slurm -s tests/plugins/slurm_samples/basic
```

Use `-s /path/to/traces` for your own saved output. Required filenames depend
on the backend; see the [full guide](documentation.md).

## Export or capture a snapshot

Add `-E` to export normalized cluster state as JSON. The terminal report still
prints; qtop writes `qtop_json_*.json` in its configured save directory.
For example, using the same offline fixture:

```sh
./qtop -b slurm -s tests/plugins/slurm_samples/basic -E
```

Create a reproducible anonymized support archive:

```sh
./qtop -b slurm -s tests/plugins/slurm_samples/basic -L -A -e
./qtop --load-sample /path/to/qtop_sample_anonymous.tar
```

For a live capture, omit `-s tests/plugins/slurm_samples/basic` and select your
scheduler. `-A` is experimental and requires `-e`. Loading the archive restores
parsed state and presentation without scheduler commands. Review the
[masking limitations](documentation.md#anonymization-scope) before sharing it.

Use `-LL` for an expanded archive with Python sources, help text, web assets,
a launcher and the license. Without `-A`, it also includes bundled
`qtopconf.yaml` defaults and `qtop-effective-config.json` with merged configuration
and command-line overrides. The JSON is diagnostic data, not a `--config` file.
Anonymous archives omit both files and retain sanitized configuration in
`qtop-snapshot.json`; replay them using an installed qtop and `--load-sample`.

For parser debugging, add `--masked-traces` to include experimental masked
scheduler-format files, with omissions listed in `masked-traces/report.json`.
Add `--raw-companion` to preserve exact scheduler inputs in a **separate private**
`qtop_raw_traces_*.tar` before parsing. Both require `-L -A -e`; see the
[trace capture details](documentation.md#anonymization-scope).

Output defaults to `/tmp/qtop_results_$USER`. Override it with
`-o savepath=/path/to/private/directory`; the directory must be private and
owned by your user. At exit, qtop prints generated archive paths on stderr,
including anonymous captures and runs with `-O`.

## Find help

- `./qtop --help` lists command-line options; `-d` adds debug diagnostics.
- With `-d`, the debug log includes copy-pasteable POSIX shell collection
  commands with quoted arguments, working directories and output paths.
- [Full user guide](documentation.md): configuration, navigation and troubleshooting.
- [README](../README.md#installation): installation options.
- [Contributing](../CONTRIBUTING.md): development and validation.
