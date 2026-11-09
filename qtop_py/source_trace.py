## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Conservative, opt-in masking of scheduler source formats."""

import copy
import io
import json
import re
import tarfile
from typing import Any, Container, Dict, Mapping


class TraceMasker:
    def __init__(self, mappings: Dict[str, Dict[str, str]]) -> None:
        self.mappings = copy.deepcopy(mappings)

    def __call__(self, value: Any, kind: str = "text") -> str:
        text = str(value)
        if not text:
            return text
        mapping = self.mappings.setdefault(kind, {})
        if text not in mapping:
            identity = re.fullmatch(r"([0-9]+)((?:_|\+|\[)[0-9,\[\]_%+\-]*)", text) if kind == "job" else None
            if identity:
                mapping[text] = self(identity[1], "job") + identity[2]
                return mapping[text]
            index = len(mapping)
            mapping[text] = str(100000 + index) if kind == "job" else "%s_trace_%s" % (kind, index)
        return mapping[text]

    @staticmethod
    def public(value: str, pattern: str) -> str:
        if re.fullmatch(pattern, value) is None:
            raise ValueError("unsupported scalar")
        return value

    def words(self, value: str, kind: str = "text") -> str:
        return re.sub(r"\S+", lambda match: self(match.group(), kind), value)

    def structured(self, value: Any, labels: Container[str]) -> Any:
        if isinstance(value, dict):
            return {key if key in labels else self(key, "field"): self.structured(item, labels) for key, item in value.items()}
        if isinstance(value, list):
            return [self.structured(item, labels) for item in value]
        return value if value is None or isinstance(value, bool) else self(value)


def add_text(archive: tarfile.TarFile, name: str, text: str) -> None:
    data = text.encode("utf-8")
    member = tarfile.TarInfo(name)
    member.mode, member.size = 0o600, len(data)
    archive.addfile(member, io.BytesIO(data))


def save_masked_traces(archive: tarfile.TarFile, backend: Any, filenames: Mapping[str, str], mappings: Dict[str, Dict[str, str]]) -> None:
    mask = TraceMasker(mappings)
    report: Dict[str, Any] = {"included": [], "omitted": [], "limitations": "Fields may be redacted or reformatted; use the raw companion for exact input bytes."}
    for key, path in filenames.items():
        try:
            with open(path, encoding="utf-8") as stream:
                name, text = backend.mask_source_trace(key, stream.read(), mask)
        except (OSError, UnicodeError, ValueError, KeyError, TypeError, AttributeError, SyntaxError):
            # Do not expose original paths, failed records, or parser exceptions.
            report["omitted"].append(key)
            continue
        add_text(archive, "masked-traces/" + name, text)
        report["included"].append(key)
    add_text(archive, "masked-traces/report.json", json.dumps(report, indent=2) + "\n")
