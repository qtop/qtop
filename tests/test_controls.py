## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Watch-mode keyboard behavior through MVU_update/MVU_effects (as in main()), using real viewport bounds."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qtop_py import qtop
from qtop_py.mvu_query import WNFilter
from qtop_py.ui.viewport import Viewport


def make_controls(color="OFF"):
    view = Viewport(20, 30)
    view.set_term_size(10, 20)
    view.max_height, view.max_width = 100, 200
    config = {"transpose_wn_matrices": False, "rem_empty_corelines": 0}
    args = SimpleNamespace(COLOR=color, EXPAND_QUEUES=False)
    model = qtop.MVU_Model(config, args, view, display_overrides={"force_names": False}, help_file="help.txt")
    return SimpleNamespace(model=model, cluster=SimpleNamespace(WNFilter=WNFilter, worker_nodes=[]))


@pytest.fixture
def controls():
    return make_controls()


def press(controls, key):
    controls.model, effects = qtop.MVU_update(controls.model, key)
    qtop.MVU_effects(controls.model, effects, controls.cluster, [0, 0, 0, 0], [0, 0, 0, 0])


def view(controls):
    return controls.model.viewport


def overrides(controls):
    return controls.model.display_overrides


@pytest.mark.parametrize(
    "key, vertical, horizontal",
    (
        ("j", 29, 30),  # a 10-row terminal pages by 9 lines: its last row is for notices
        (" ", 29, 30),
        ("k", 11, 30),
        ("\x7f", 11, 30),
        ("l", 20, 40),
        ("h", 20, 20),
        ("$", 20, 180),
        ("0", 20, 0),
        ("J", 91, 30),
        ("G", 91, 30),
        ("K", 0, 30),
        ("g", 0, 30),
        ("R", 0, 0),
    ),
)
def test_movement_keys_respect_viewport(controls, key, vertical, horizontal):
    press(controls, key)
    assert (view(controls).v_start, view(controls).h_start) == (vertical, horizontal)


@pytest.mark.parametrize("key", ("j", "G"))
def test_downward_movement_stops_at_bottom(controls, capsys, key):
    view(controls).scroll_bottom()
    press(controls, key)
    assert view(controls).v_start == 91
    assert "Staying put" in capsys.readouterr().out


@pytest.mark.parametrize("key, setting", (("t", "transpose_wn_matrices"), ("F", "force_names"), ("Q", "queue_summary_expanded")))
def test_display_toggles_are_reversible(controls, key, setting):
    press(controls, key)
    assert overrides(controls)[setting] is True
    press(controls, key)
    assert overrides(controls)[setting] is False


def test_color_mapping_and_empty_coreline_modes_cycle(controls):
    press(controls, "m")
    assert overrides(controls)["core_coloring"] == "queue_to_color"
    press(controls, "m")
    assert overrides(controls)["core_coloring"] == "user_to_color"
    for expected in (1, 2, 0):
        press(controls, "r")
        assert overrides(controls)["rem_empty_corelines"] == expected


def test_help_returns_to_cluster_and_quit_leaves_cleanup_to_owner(controls):
    press(controls, "?")
    assert overrides(controls)["output_fp"] == "help.txt"
    press(controls, "?")
    assert "output_fp" not in overrides(controls)
    with pytest.raises(SystemExit) as stopped:
        press(controls, "q")
    assert stopped.value.code == 0
    assert "web" not in vars(qtop)  # no module-level server for the controls to stop; RunResources owns it


def test_menus_use_the_models_colour_setting(monkeypatch, capsys):
    answers = iter([""])
    monkeypatch.setattr("builtins.input", lambda *_args: next(answers))
    monkeypatch.setattr(qtop, "_set_terminal_attrs", Mock())
    press(make_controls(color="ON"), "s")
    assert "\x1b[" in capsys.readouterr().out  # -c ON: the menu numbers are coloured


@pytest.mark.parametrize("answers, expected", ((["invalid", "31"], ["sort by all numbers", "sort by nodename-notnum"]), ([""], [])))
def test_sort_menu_handles_invalid_input_and_cancel(controls, monkeypatch, answers, expected):
    answers = iter(answers)
    monkeypatch.setattr("builtins.input", lambda *_args: next(answers))
    terminal = Mock()
    monkeypatch.setattr(qtop, "_set_terminal_attrs", terminal)
    press(controls, "s")
    assert [entry[0] for entry in overrides(controls)["user_sort"]] == expected
    assert (view(controls).v_start, view(controls).h_start) == (0, 0)
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
    assert overrides(controls)[setting] == expected
    assert isinstance(controls.cluster.wn_filter, WNFilter)
    assert view(controls).v_start == view(controls).h_start == 0
    assert terminal.call_count == 2


@pytest.mark.parametrize("key, setting", (("f", "filtering"), ("H", "highlight")))
@pytest.mark.parametrize("answer", ("", "not-a-number", "99"))
def test_filter_and_highlight_menus_cancel_invalid_commands(controls, monkeypatch, key, setting, answer):
    monkeypatch.setattr("builtins.input", lambda *_args: answer)
    monkeypatch.setattr(qtop, "_set_terminal_attrs", Mock())
    press(controls, key)
    assert overrides(controls)[setting] == []


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
    before = dict(overrides(controls))
    press(controls, "z")
    assert (view(controls).v_start, view(controls).h_start) == (20, 30)
    assert overrides(controls) == before
