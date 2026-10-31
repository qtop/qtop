# SPDX-License-Identifier: MIT
"""Bounded host-range expansion without dependencies on scheduler plugins."""

import logging
import re
from collections import OrderedDict
from typing import List, Optional


class HostlistParser:
    # Bound both numeric ranges and Cartesian products from untrusted traces.
    MAX_EXPANDED_NODES = 100000

    @classmethod
    def expand_nodelist(cls, nodelist: Optional[str]) -> List[str]:
        """Expand comma-separated host groups; reject malformed lists as a unit.

        Commas inside brackets separate numeric choices. Unbalanced brackets,
        empty groups, descending ranges and oversized expansions are unusable;
        return no hosts rather than assigning a job to an invented hostname.
        """
        nodelist = nodelist.strip() if nodelist else ""
        if not nodelist or nodelist.startswith("("):
            return []

        try:
            groups = []
            depth = 0
            start = 0
            for index, char in enumerate(nodelist):
                if char == "[":
                    if depth:
                        raise ValueError("nested brackets")
                    depth = 1
                elif char == "]":
                    if not depth:
                        raise ValueError("unmatched closing bracket")
                    depth = 0
                elif char == "," and not depth:
                    groups.append(nodelist[start:index])
                    start = index + 1
            if depth:
                raise ValueError("unmatched opening bracket")
            groups.append(nodelist[start:])

            expanded = []
            for group in groups:
                if not group or re.search(r"\s", group):
                    raise ValueError("empty or whitespace-containing host group")
                pending = [group]
                while any("[" in item for item in pending):
                    next_pending = []
                    for item in pending:
                        next_pending.extend(cls._expand_first_range(item))
                        if len(expanded) + len(next_pending) > cls.MAX_EXPANDED_NODES:
                            raise ValueError("too many expanded hosts")
                    pending = next_pending
                expanded.extend(pending)
                if len(expanded) > cls.MAX_EXPANDED_NODES:
                    raise ValueError("too many expanded hosts")
            return list(OrderedDict.fromkeys(expanded))
        except ValueError as error:
            logging.warning("Ignoring invalid Slurm nodelist %r: %s", nodelist, error)
            return []

    @classmethod
    def _expand_first_range(cls, value: str) -> List[str]:
        match = re.search(r"\[([^\]]+)\]", value)
        if not match:
            if "[" in value:
                raise ValueError("empty host range")
            return [value]

        prefix = value[: match.start()]
        suffix = value[match.end() :]
        choices = []
        for token in match.group(1).split(","):
            if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", token):
                raise ValueError("invalid numeric host range")
            if "-" not in token:
                choices.append(token)
                continue
            start, end = token.split("-", 1)
            first, last = int(start), int(end)
            if last < first:
                raise ValueError("descending host range")
            if len(choices) + last - first + 1 > cls.MAX_EXPANDED_NODES:
                raise ValueError("too many expanded hosts")
            width = len(start)
            choices.extend(str(number).zfill(width) for number in range(first, last + 1))
        return [prefix + choice + suffix for choice in choices]
