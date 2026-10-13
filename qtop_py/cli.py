# SPDX-FileContributor: Sebastian Schnorbus <sebastian.schnorbus@hpe.com>

##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-FileCopyrightText: 2016 Fotis Georgatos
## SPDX-FileCopyrightText: 2016 Sotiris Fragkiskos
## SPDX-FileCopyrightText: 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import re
import sys

from qtop_py.qtop import cli_main

if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(cli_main())
