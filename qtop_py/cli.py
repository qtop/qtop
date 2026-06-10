##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import re
import sys
from qtop_py.qtop import InvalidScheduler, NoSchedulerFound, SchedulerNotSpecified, main


def cli_main():
    try:
        return main() or 0
    except SchedulerNotSpecified:
        # fmt: off
        sys.stderr.write(
            "No scheduler could be auto-detected. "
            "Select one with -b/--batchSystem, QTOP_SCHEDULER, or qtopconf.yaml.\n"
        )
        # fmt: on
        return 1
    except (InvalidScheduler, NoSchedulerFound) as exc:
        if str(exc):
            sys.stderr.write("%s\n" % exc)
        return 1


if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(cli_main())
