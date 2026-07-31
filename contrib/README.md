# contrib - historical reference material

This directory holds material kept for **reference and provenance only**. It is
not part of the qtop runtime, is not imported by `qtop_py`, is not exercised by
the test suite, and is not shipped in the sdist or the wheel.

## `qtop.sh` - the original shell qtop

`qtop.sh` is the historic bash implementation of qtop that the current Python
tool evolved from, added verbatim so the origin of the tool stays inspectable
inside the repository (qtop/qtop#244). It is the last shell release: its own
header already points readers at the Python rewrite.

| | |
| --- | --- |
| Release | qtop-53 |
| Revision | `$Id: qtop 3053 2012-09-14 13:42:46Z fotis $` |
| Packaged | 2012-12-11 |
| Author | Fotis Georgatos `<fotis@cern.ch>` |
| Copyright | (C) 2008, 2009, 2010, ETH Zuerich / CSCS; (C) 2012, University of Luxembourg / LCSB |
| Retrieved from | `qtop-53/qtop` inside <https://fotis.web.cern.ch/fotis/QTOP/qtop.tar.gz> |
| sha256 | `fcffd7705bafb0d35f27ef1208e312974afc39d439fd195763230472b3ed8103` |

The bytes are unchanged from that tarball; only the name gained a `.sh` suffix,
as the issue asked for. `LICENSE` is likewise the tarball's own licence file,
copied verbatim - it is the "LICENSE file" the script header refers to.

The same tarball also carries `qtop.conf`, `qtop.colormap`, `qtop.man`,
`ansi2html.sh`, `qtop4cron` and `qtop4oar`. Those are left out on purpose: the
issue asked for the script itself, and the tarball remains one download away for
anyone who wants the rest.

### Licensing - please read before redistributing

**qtop itself is MIT licensed. `qtop.sh` is not.** Its header places it under
the *GNU General Public License version 2 or later*, and its copyright is held
by ETH Zuerich / CSCS and the University of Luxembourg rather than by the qtop
authors. The GPL-2.0 text ships next to it as `LICENSE`, as that licence
requires of anyone distributing a verbatim copy.

To keep this from surprising anyone downstream, the file is deliberately
isolated:

- it lives at the repository root under `contrib/`, **outside** the `qtop_py`
  package, so it is unreachable from an installed qtop;
- `pyproject.toml` (`packages = ["qtop_py"]`) and `setup.py` (`find_packages`)
  never see it;
- `MANIFEST.in` uses explicit includes, so it stays out of the sdist too.

The practical consequence: `pip install qtop`, the built wheel and the built
sdist all remain MIT-only, exactly as before. Only people who clone the git
repository receive this file, and they receive it under GPL-2.0-or-later.
Anyone redistributing a clone or a `git archive` of the repository is
redistributing a GPL-2.0-or-later file and should honour that licence for it.

### Do not run this on a live cluster

The script is prototype-grade 2012 code, kept for reading rather than for use;
its own header says *"THIS CODE IS WORK OF PROTOTYPING NATURE, THE REGULAR
'AS-IS' CONDITIONS APPLY"*. It targets Torque/PBS only, shells out to `qstat`,
`qstat -q` and `pbsnodes -a`, and offers to fetch a colormap over plain HTTP
from a URL that no longer serves one. Use the Python `qtop` for anything real.
