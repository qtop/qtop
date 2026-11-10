## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Select one coherent scheduler capture without extracting archive paths."""

import logging
import os
import re
import shutil
import tarfile
from collections import defaultdict
from pathlib import PurePosixPath
from typing import DefaultDict, Dict, Iterable, List, Mapping, Tuple

MAX_MEMBERS = 10000
MAX_TRACE_BYTES = 256 * 1024 * 1024
MAX_SET_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024


def load_trace_archive(path: str, directory: str, configured: Mapping[str, str], optional: Iterable[str] = ()) -> Dict[str, str]:
    """Extract one coherent capture of the configured traces into directory; return their paths by key."""
    marker = "QTOP_CAPTURE_SUFFIX"
    patterns = {}
    destinations = {}
    specificity = {}
    for key, specification in configured.items():
        template = specification.split(", ", 1)[0].strip()
        name = os.path.basename(template % {"savepath": directory, "pid": marker})
        patterns[key] = re.compile("^" + re.escape(name).replace(marker, "(?P<capture>.*)") + "$")
        destinations[key] = os.path.join(directory, name.replace(marker, ""))
        specificity[key] = len(name.replace(marker, ""))
    if len(set(destinations.values())) != len(destinations):
        raise ValueError("configured trace filenames must be distinct")
    required = set(configured) - set(optional)
    cohorts: DefaultDict[Tuple[str, str], DefaultDict[str, List[tarfile.TarInfo]]] = defaultdict(lambda: defaultdict(list))
    with tarfile.open(path, "r:*") as archive:
        for index, member in enumerate(archive):
            if index >= MAX_MEMBERS or member.offset_data + member.size > MAX_ARCHIVE_BYTES:
                raise ValueError("trace archive exceeds member or size limits")
            member_path = PurePosixPath(member.name)
            if (
                not member.isfile()
                or member.sparse is not None
                or member_path.is_absolute()
                or ".." in member_path.parts
                or "\\" in member.name
                or any(":" in part for part in member_path.parts)
            ):
                continue
            matches = []
            for key, pattern in patterns.items():
                match = pattern.fullmatch(member_path.name)
                if match or member_path.name == key:
                    suffix = (match.groupdict().get("capture") or "") if match else ""
                    matches.append((len(member_path.name) + 1 if member_path.name == key else specificity[key], key, suffix))
            if matches:
                _, key, suffix = max(matches)
                cohorts[(str(member_path.parent), suffix)][key].append(member)
        eligible = [(len(files), cohort, files) for cohort, files in cohorts.items() if required <= set(files) and all(len(members) == 1 for members in files.values())]
        if not eligible:
            raise ValueError("no unambiguous complete scheduler trace set; use --load-sample for normalized snapshots")
        _, cohort, selected = max(eligible, key=lambda candidate: (candidate[0], candidate[1]))
        members = {key: entries[0] for key, entries in selected.items()}
        if any(member.size > MAX_TRACE_BYTES for member in members.values()) or sum(member.size for member in members.values()) > MAX_SET_BYTES:
            raise ValueError("selected trace set exceeds size limits")
        logging.debug("Trace archive set: directory=%r capture suffix=%r", *cohort)
        paths = {}
        for key in sorted(members):
            member = members[key]
            target = destinations[key]
            source = archive.extractfile(member)
            if source is None:
                raise ValueError("selected trace data is missing")
            with source, open(target, "xb") as output:
                shutil.copyfileobj(source, output, length=64 * 1024)
            logging.debug("Trace archive member: %s -> %s", member.name, target)
            paths[key] = target
    return paths
