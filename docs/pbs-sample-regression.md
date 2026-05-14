# PBS sample regression validation

This change was validated against the public PBS sample corpus from:

https://github.com/fgeorgatos/qtop-test-repo/tree/master/qtop5/results

The regression target is to render at least 100 PBS sample directories without
crashing. The local validation run covered the first 120 sample directories and
all 120 completed successfully.

Fixed crash classes:

- malformed qstat rows now get skipped instead of aborting parsing
- empty PBS worker-node dictionaries no longer raise while calculating matrix
  attributes
- jobs reported by pbsnodes but missing from qstat are skipped with a warning
- mixed numeric and non-numeric worker-node names no longer raise during remap
  decisions

To run the external sample regression:

```sh
git clone https://github.com/fgeorgatos/qtop-test-repo /tmp/qtop-test-repo
QTOP_PBS_SAMPLE_DIR=/tmp/qtop-test-repo/qtop5/results make test-pbs-samples
```

The normal test entry point remains:

```sh
make test
```
