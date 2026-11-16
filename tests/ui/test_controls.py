## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Complete watch keys, burst input and literal navigation prompts."""

import os
import sys
from types import SimpleNamespace

import pytest

from qtop_py import qtop
from qtop_py.ui import controls
from qtop_py.ui.controls import KeyboardInput
from qtop_py.ui.viewport import Viewport


@pytest.fixture
def keyboard(monkeypatch):
    def make(data):
        pending = bytearray(data)
        stream = SimpleNamespace(fileno=lambda: 42)
        monkeypatch.setattr(controls.os, "read", lambda *_args: bytes([pending.pop(0)]) if pending else b"")
        monkeypatch.setattr(controls.select, "select", lambda *_args: ([stream] if pending else [], [], []))
        return KeyboardInput(stream)

    return make


@pytest.mark.parametrize(
    "data,expected",
    [
        (b"FQq", ["F", "Q", "q"]),
        (b"1Gq", ["home", "q"]),
        (b"1xq", ["1", "x", "q"]),
        (b"1", ["1"]),
        (b"\x1b[A\x1b[B\x1b[C\x1b[D", ["up", "down", "right", "left"]),
        (b"\x1bOH\x1bOF", ["home", "end"]),
        (b"\x1b[1~\x1b[4~\x1b[5~\x1b[6~", ["home", "end", "pageup", "pagedown"]),
        (b"\x1b[7~\x1b[8~\x1b[H\x1b[F", ["home", "end", "home", "end"]),
        (b"\x1bq", ["\x1b", "q"]),
        (b"\x1b", ["\x1b"]),
        (b"\x1b[999~q", ["", "q"]),
        ("\u03bbq".encode(), ["\u03bb", "q"]),
    ],
    ids=(
        "burst",
        "first-line-chord",
        "bare-one-and-next-key",
        "bare-one",
        "arrows",
        "ss3",
        "tilde-keys",
        "home-end",
        "escape-keeps-next-key",
        "escape",
        "unknown-sequence",
        "utf8",
    ),
)
def test_keyboard_decodes_complete_keys_without_losing_following_input(keyboard, data, expected):
    reader = keyboard(data)
    assert [reader.read() for _ in expected] == expected


def test_literal_prompts_do_not_interpret_one_g_as_navigation(keyboard):
    reader = keyboard(b"1G")
    assert reader.read(navigation=False) == "1"
    assert reader.read(navigation=False) == "G"


def test_keyboard_eof_exits_instead_of_spinning(keyboard):
    with pytest.raises(EOFError):
        keyboard(b"").read()


def test_pending_key_is_dispatched_even_without_descriptor_readiness(keyboard, monkeypatch):
    reader = keyboard(b"\x1bq")
    assert reader.read() == "\x1b"
    monkeypatch.setattr(qtop.select, "select", lambda *_args: pytest.fail("pending keys must bypass select"))
    assert qtop.wait_for_keypress_or_autorefresh(Viewport(), {}, (10, 40), 0, reader) == "q"


@pytest.mark.skipif(os.name != "posix", reason="select on terminal/pipe descriptors requires POSIX")
def test_a_burst_of_keys_is_ready_on_each_successive_watch_cycle(monkeypatch):
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"FQq")
        with os.fdopen(read_fd, "r") as stream:
            monkeypatch.setattr(sys, "stdin", stream)
            reader = KeyboardInput(stream)
            assert [qtop.wait_for_keypress_or_autorefresh(Viewport(), {}, (10, 40), 0, reader) for _ in range(3)] == ["F", "Q", "q"]
    finally:
        os.close(write_fd)


@pytest.mark.parametrize(
    "action,typed,start,expected",
    [
        ("line", "42\r", 0, 41),
        ("line", "999\r", 0, 96),
        ("line", "0\r", 5, 5),
        ("line", "bad\r", 5, 5),
        ("line", "4x\x7f2\r", 0, 41),
        ("line", "99\x1542\r", 0, 41),
        ("line", "42\x1b", 5, 5),
        ("search", "needle\r", 0, 2),
        ("search", "ij12needle\r", 0, 1),
        ("search", "needle\r", 5, 2),
        ("search", "missing\r", 5, 5),
        ("search", "\r", 0, 2),
    ],
    ids=("line", "clamped", "zero", "invalid", "backspace", "clear", "cancel", "search", "cross-wrap", "wrap-search", "not-found", "repeat"),
)
def test_navigation_prompts_use_displayed_rows_and_leave_invalid_input_alone(tmp_path, keyboard, action, typed, start, expected):
    source = tmp_path / "frame.out"
    source.write_text("top\nabcdefghij12\x1b[31mneedle\x1b[0m\nlast\n")
    viewport = Viewport()
    viewport.set_term_size(5, 10)
    viewport.max_height, viewport.v_start = 100, start
    model = SimpleNamespace(viewport=viewport, display_overrides={"search_text": "needle"})
    qtop.navigate_prompt(model, keyboard(typed.encode()), action, str(source))
    assert viewport.v_start == expected


def test_notice_stays_on_its_reserved_row_in_a_narrow_terminal(capsys):
    viewport = Viewport()
    viewport.set_term_size(5, 8)
    qtop.write_notice("very long navigation notice", viewport)
    assert capsys.readouterr().out == "\r\x1b[2Kvery lon"
