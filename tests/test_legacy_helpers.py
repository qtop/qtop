## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Stdlib-only compatibility checks for Python 3.6+ and PyPy."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from xml.etree import ElementTree as etree

from qtop_py import safety
from qtop_py.plugins.sge import SGEBatchSystem, SGEStatExtractor
from qtop_py.source_trace import TraceMasker
from tools import validate_pbs_samples


class ArchivedPBSCompatibility(unittest.TestCase):
    def test_render_sample_uses_supported_subprocess_arguments(self):
        real_run = subprocess.run

        def scheduler_output(command, **kwargs):
            return real_run([sys.executable, "-c", "print('scheduler frame')"], **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with patch.object(validate_pbs_samples.subprocess, "run", side_effect=scheduler_output):
                result = validate_pbs_samples.render_sample(output / "trace", output)
            self.assertEqual(result["sample"], "trace")
            self.assertEqual((output / "trace.ans").read_text(), "scheduler frame\n")


class SchedulerXMLCompatibility(unittest.TestCase):
    def test_rejects_dtds_before_entity_expansion(self):
        declarations = [
            '<!DOCTYPE job_info [<!ENTITY x "expanded">]><job_info>&x;</job_info>',
            '<!DOCTYPE job_info [<!ENTITY a "x"><!ENTITY b "&a;&a;">]><job_info>&b;</job_info>',
            '<!DOCTYPE job_info SYSTEM "file:///nonexistent"><job_info/>',
            '<!DOCTYPE job_info [<!ENTITY x SYSTEM "file:///nonexistent">]><job_info>&x;</job_info>',
            "<!DOCTYPE job_info><job_info/>",
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qstat.xml"
            extractor = SGEStatExtractor({}, SimpleNamespace(ANONYMIZE=False), {})
            for encoding in ("utf-8", "utf-16", "utf-16-be"):
                for xml in declarations:
                    with self.subTest(encoding=encoding, xml=xml):
                        path.write_bytes(xml.encode(encoding))
                        with self.assertRaisesRegex(etree.ParseError, "DTD declarations are not supported"):
                            extractor.get_xml_tree(str(path))
                        with self.assertRaisesRegex(etree.ParseError, "DTD declarations are not supported"):
                            SGEBatchSystem({}, {}, extractor.options).mask_source_trace("sge_file", path.read_bytes(), TraceMasker({}))

    def test_predefined_entities_remain_supported(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qstat.xml"
            extractor = SGEStatExtractor({}, SimpleNamespace(ANONYMIZE=False), {})
            for encoding in ("utf-8", "utf-16", "utf-16-be"):
                with self.subTest(encoding=encoding):
                    path.write_bytes("<root>alice &amp; bob</root>".encode(encoding))
                    _, root = extractor.get_xml_tree(str(path))
                    self.assertEqual(root.text, "alice & bob")

    def test_parser_enforces_input_budgets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qstat.xml"
            path.write_text("<root><child/><child/></root>")
            extractor = SGEStatExtractor({}, SimpleNamespace(ANONYMIZE=False), {})
            for name, limit in (("MAX_INPUT_BYTES", 8), ("MAX_DEPTH", 1), ("MAX_ITEMS", 2)):
                with self.subTest(limit=name), patch.object(safety, name, limit):
                    with self.assertRaisesRegex(etree.ParseError, "XML input exceeds"):
                        extractor.get_xml_tree(str(path))
