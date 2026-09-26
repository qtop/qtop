import logging
import tarfile
from unittest.mock import Mock

import pytest

from qtop_py import fileutils


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5h", {"hours": 5}),
        ("10m", {"minutes": 10}),
        ("30s", {"seconds": 30}),
    ],
)
def test_parse_time_input(value, expected):
    assert fileutils.parse_time_input(value) == expected


@pytest.mark.parametrize("value", ["", "5d", "hours", "hm", None])
def test_parse_time_input_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="number followed by h, m, or s"):
        fileutils.parse_time_input(value)


def test_add_to_sample_uses_a_portable_archive_path(tmp_path):
    source = tmp_path / "qtop.log"
    sample_out = Mock()

    result = fileutils.add_to_sample([str(source)], sample_out, subdir="qtop_py")

    sample_out.add.assert_called_once_with(str(source), arcname="qtop_py/qtop.log")
    assert result is sample_out


@pytest.mark.parametrize("error", [tarfile.TarError("invalid archive"), OSError("file disappeared")])
def test_add_to_sample_reports_source_path_on_failure(tmp_path, caplog, error):
    source = tmp_path / "qtop.log"
    sample_out = Mock()
    sample_out.add.side_effect = error

    with caplog.at_level(logging.ERROR):
        fileutils.add_to_sample([str(source)], sample_out)

    assert str(source) in caplog.text
    assert str(error) in caplog.text
