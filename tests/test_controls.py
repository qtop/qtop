## SPDX-License-Identifier: MIT
"""Watch-mode keyboard behavior, using real viewport bounds."""

from itertools import cycle
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qtop_py import qtop
from qtop_py.ui.viewport import Viewport


@pytest.fixture
def controls(monkeypatch):
    view = Viewport(20, 30)
    view.set_term_size(10, 20)
    view.max_height, view.max_width = 100, 200
    monkeypatch.setattr(qtop, "args", SimpleNamespace(COLOR="OFF", EXPAND_QUEUES=False), raising=False)
    monkeypatch.setattr(qtop, "config", {"transpose_wn_matrices": False, "rem_empty_corelines": 0}, raising=False)
    monkeypatch.setattr(qtop, "dynamic_config", {"force_names": False}, raising=False)
    monkeypatch.setattr(qtop, "user_to_color", {}, raising=False)
    monkeypatch.setattr(qtop, "change_mapping", cycle((("queue_to_color", "queue"), ("user_to_color", "user"))), raising=False)
    monkeypatch.setattr(qtop, "h_counter", cycle((0, 1)), raising=False)
    monkeypatch.setattr(qtop, "help_main_switch", ["help.txt", "output.out"], raising=False)
    web = Mock()
    monkeypatch.setattr(qtop, "web", web, raising=False)
    cluster = SimpleNamespace(WNFilter=qtop.WNFilter, worker_nodes=[])
    return view, cluster, web


def press(controls, key):
    view, cluster, _web = controls
    qtop.control_qtop(view, key, cluster, [0, 0, 0, 0], [0, 0, 0, 0])


@pytest.mark.parametrize(
    "key, vertical, horizontal",
    (
        ("j", 30, 30),
        (" ", 30, 30),
        ("k", 10, 30),
        ("\x7f", 10, 30),
        ("l", 20, 40),
        ("h", 20, 20),
        ("$", 20, 180),
        ("0", 20, 0),
        ("J", 90, 30),
        ("G", 90, 30),
        ("K", 0, 30),
        ("g", 0, 30),
        ("R", 0, 0),
    ),
)
def test_movement_keys_respect_viewport(controls, key, vertical, horizontal):
    press(controls, key)
    view, _cluster, _web = controls
    assert (view.v_start, view.h_start) == (vertical, horizontal)


@pytest.mark.parametrize("key", ("j", "G"))
def test_downward_movement_stops_at_bottom(controls, capsys, key):
    view, _cluster, _web = controls
    view.scroll_bottom()
    press(controls, key)
    assert view.v_start == 90
    assert "Staying put" in capsys.readouterr().out


@pytest.mark.parametrize("key, setting", (("t", "transpose_wn_matrices"), ("F", "force_names"), ("Q", "queue_summary_expanded")))
def test_display_toggles_are_reversible(controls, key, setting):
    press(controls, key)
    assert qtop.dynamic_config[setting] is True
    press(controls, key)
    assert qtop.dynamic_config[setting] is False


def test_color_mapping_and_empty_coreline_modes_cycle(controls):
    press(controls, "m")
    assert qtop.dynamic_config["core_coloring"] == "queue_to_color"
    press(controls, "m")
    assert qtop.dynamic_config["core_coloring"] == "user_to_color"
    for expected in (1, 2, 0):
        press(controls, "r")
        assert qtop.dynamic_config["rem_empty_corelines"] == expected


def test_help_returns_to_cluster_and_quit_leaves_cleanup_to_owner(controls):
    press(controls, "?")
    assert qtop.dynamic_config["output_fp"] == "help.txt"
    press(controls, "?")
    assert "output_fp" not in qtop.dynamic_config
    with pytest.raises(SystemExit) as stopped:
        press(controls, "q")
    assert stopped.value.code == 0
    controls[2].stop.assert_not_called()


@pytest.mark.parametrize("answers, expected", ((["invalid", "31"], ["sort by all numbers", "sort by nodename-notnum"]), ([""], [])))
def test_sort_menu_handles_invalid_input_and_cancel(controls, monkeypatch, answers, expected):
    answers = iter(answers)
    monkeypatch.setattr("builtins.input", lambda *_args: next(answers))
    terminal = Mock()
    monkeypatch.setattr(qtop, "_set_terminal_attrs", terminal)
    press(controls, "s")
    assert [entry[0] for entry in qtop.dynamic_config["user_sort"]] == expected
    assert (controls[0].v_start, controls[0].h_start) == (0, 0)
    assert terminal.call_count == 2


@pytest.mark.parametrize(
    "key, setting, answers, expected",
    (
        ("f", "filtering", ["9", "^node", "", "10", "debug", "", ""], [{"include_name_patterns": ["^node"]}, {"include_queues": ["debug"]}]),
        ("H", "highlight", ["2", "alice", "", "3", "debug", "", ""], [{"or_include_user_pat": ["alice"]}, {"or_include_queue": ["debug"]}]),
    ),
)
def test_filter_and_highlight_menus_preserve_selected_rules(controls, monkeypatch, key, setting, answers, expected):
    answers = iter(answers)
    monkeypatch.setattr("builtins.input", lambda *_args: next(answers))
    terminal = Mock()
    monkeypatch.setattr(qtop, "_set_terminal_attrs", terminal)
    press(controls, key)
    assert qtop.dynamic_config[setting] == expected
    assert isinstance(controls[1].wn_filter, qtop.WNFilter)
    assert controls[0].v_start == controls[0].h_start == 0
    assert terminal.call_count == 2


@pytest.mark.parametrize("key, setting", (("f", "filtering"), ("H", "highlight")))
@pytest.mark.parametrize("answer", ("", "not-a-number", "99"))
def test_filter_and_highlight_menus_cancel_invalid_commands(controls, monkeypatch, key, setting, answer):
    monkeypatch.setattr("builtins.input", lambda *_args: answer)
    monkeypatch.setattr(qtop, "_set_terminal_attrs", Mock())
    press(controls, key)
    assert qtop.dynamic_config[setting] == []


@pytest.mark.parametrize("key", ["s", "f", "H"])
@pytest.mark.parametrize("failure", [EOFError, KeyboardInterrupt])
def test_menu_interruption_restores_terminal(controls, monkeypatch, key, failure):
    def interrupt(*args):
        raise failure()

    monkeypatch.setattr("builtins.input", interrupt)
    terminal = Mock()
    monkeypatch.setattr(qtop, "_set_terminal_attrs", terminal)
    with pytest.raises(failure):
        press(controls, key)
    assert terminal.call_count == 2
    assert terminal.call_args[0] == ([0, 0, 0, 0],)


def test_unknown_key_leaves_view_and_settings_unchanged(controls):
    before = dict(qtop.dynamic_config)
    press(controls, "z")
    assert (controls[0].v_start, controls[0].h_start) == (20, 30)
    assert qtop.dynamic_config == before
