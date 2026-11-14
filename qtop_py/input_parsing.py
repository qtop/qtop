# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
"""Pure parsing of replay times and configured account field expressions."""

import datetime
import re
from typing import Optional

from qtop_py.constants import QTOPCONF_YAML


def extract_regex_detail(regex: Optional[str], field: str) -> str:
    """Return field, or the group a re.search(...).group(n) expression from qtopconf selects from it."""
    expression = (regex or "").strip()
    if not expression:
        return field.strip()

    match = re.match(r"^re\.search\((?P<quote>['\"])(?P<pattern>.*)(?P=quote),\s*field\)\.group\((?P<group>\d+)\)$", expression, re.DOTALL)
    if not match:
        raise ValueError("Unsupported user detail regex expression in %s" % QTOPCONF_YAML)

    found = re.search(match.group("pattern"), field)
    if found is None:
        raise ValueError("User detail regex did not match field")
    return found.group(int(match.group("group")))


def get_date_obj_from_str(s: str, now: datetime.datetime) -> datetime.datetime:
    """Parse a -R replay time into a datetime; the separators :/- are allowed anywhere.

    yyyymmddTHHMMSS, e.g. 20161118T182300
    mmddTHHMM, e.g. 1118T1823: the current year is implied.
    HHMM, e.g. 1823: today, or yesterday if that time is still ahead.
    """
    s = "".join([x for x in s if x not in ":/-"])
    if "T" in s and len(s) == 15:
        inp_datetime = datetime.datetime.strptime(s, "%Y%m%dT%H%M%S")
    elif len(s) == 4:
        _inp_datetime = datetime.datetime.strptime(s, "%H%M")
        _inp_datetime = now.replace(hour=_inp_datetime.hour, minute=_inp_datetime.minute, second=0)
        inp_datetime = _inp_datetime if now > _inp_datetime else _inp_datetime - datetime.timedelta(days=1)
    elif len(s) == 9:
        _inp_datetime = datetime.datetime.strptime(s, "%m%dT%H%M")
        inp_datetime = _inp_datetime.replace(year=now.year, second=0)
    else:
        raise ValueError("The datetime format provided is incorrect.\nTry one of the formats: yyyymmddTHHMMSS, HHMM, mmddTHHMM.")
    return inp_datetime
