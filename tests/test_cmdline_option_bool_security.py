import pytest

from qtop_py.qtop import update_config_with_cmdline_vars


class _Args:
    OPTION = []
    TRANSPOSE = False
    REM_EMPTY_CORELINES = False


def _run_options(options, config):
    _Args.OPTION = list(options)
    return update_config_with_cmdline_vars(_Args, dict(config))


def test_cmdline_false_literal_is_parsed_as_boolean_false():
    updated = _run_options(["safe_flag=False"], {"rem_empty_corelines": 0, "safe_flag": True})

    assert updated["safe_flag"] is False


def test_cmdline_value_containing_false_substring_is_not_executed(tmp_path):
    sentinel = tmp_path / "cmdline-false-substring-executed"
    payload = "__import__('pathlib').Path(%r).touch() or False" % str(sentinel)

    updated = _run_options(["danger=%s" % payload], {"rem_empty_corelines": 0})

    assert updated["danger"] == payload
    assert not sentinel.exists()
