## SPDX-License-Identifier: MIT
"""Conservative, opt-in masking of scheduler source formats."""

import copy
import io
import json
import re
import tarfile


class TraceMasker:
    def __init__(self, mappings):
        self.mappings = copy.deepcopy(mappings)

    def __call__(self, value, kind="text"):
        value = str(value)
        if not value:
            return value
        mapping = self.mappings.setdefault(kind, {})
        if value not in mapping:
            identity = re.fullmatch(r"([0-9]+)((?:_|\+|\[)[0-9,\[\]_%+\-]*)", value) if kind == "job" else None
            if identity:
                mapping[value] = self(identity[1], "job") + identity[2]
                return mapping[value]
            index = len(mapping)
            mapping[value] = str(100000 + index) if kind == "job" else "%s_trace_%s" % (kind, index)
        return mapping[value]

    @staticmethod
    def public(value, pattern):
        if re.fullmatch(pattern, value) is None:
            raise ValueError("unsupported scalar")
        return value

    def words(self, value, kind="text"):
        return re.sub(r"\S+", lambda match: self(match.group(), kind), value)

    def structured(self, value, labels):
        if isinstance(value, dict):
            return {key if key in labels else self(key, "field"): self.structured(item, labels) for key, item in value.items()}
        if isinstance(value, list):
            return [self.structured(item, labels) for item in value]
        return value if value is None or isinstance(value, bool) else self(value)


def add_text(archive, name, text):
    data = text.encode("utf-8")
    member = tarfile.TarInfo(name)
    member.mode, member.size = 0o600, len(data)
    archive.addfile(member, io.BytesIO(data))


def save_masked_traces(archive, backend, filenames, mappings):
    mask = TraceMasker(mappings)
    report = {"included": [], "omitted": [], "limitations": "Fields may be redacted or reformatted; use the raw companion for exact input bytes."}
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
