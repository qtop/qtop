# SPDX-License-Identifier: MIT
"""Run with unittest in the dependency-light Python 3.6 lane as well as pytest."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
