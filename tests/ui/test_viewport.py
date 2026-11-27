## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import re
from pathlib import Path

import pytest

from qtop_py.ui.viewport import Viewport, terminal_rows


def test_defaults():
    viewport = Viewport()
    assert 0 == viewport.h_start
    assert 0 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 0 == viewport.v_stop


def test_after_set_term_size():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    assert 53 == viewport.v_term_size
    assert 176 == viewport.h_term_size


# One action from a start position: (v_start, h_start), terminal (rows, columns),
# output (max_height, max_width; 0 = empty), action, expected (v_start, v_stop, h_start, h_stop).
# A page is rows - 1 lines: the last terminal row is kept for notices.
SCROLLS = {
    "left-at-origin": ((0, 0), (53, 176), (200, 200), "scroll_left", (0, 53, 0, 176)),
    "right-half-page": ((0, 0), (53, 176), (200, 400), "scroll_right", (0, 53, 88, 264)),
    "right-clamped-to-edge": ((0, 0), (53, 176), (200, 200), "scroll_right", (0, 53, 24, 200)),
    "right-from-middle": ((0, 100), (53, 176), (400, 400), "scroll_right", (0, 53, 188, 364)),
    "right-start-past-edge": ((0, 100), (53, 176), (400, 200), "scroll_right", (0, 53, 24, 200)),
    "right-start-far-past-edge": ((0, 199), (53, 176), (400, 200), "scroll_right", (0, 53, 24, 200)),
    "right-nowhere-to-go": ((0, 224), (53, 176), (200, 400), "scroll_right", (0, 53, 224, 400)),
    "far-right-touches-edge": ((200, 200), (53, 176), (400, 400), "scroll_far_right", (200, 253, 224, 400)),
    "down-one-page": ((0, 0), (50, 120), (200, 400), "scroll_down", (49, 99, 0, 120)),
    "down-empty-output": ((0, 0), (53, 176), (0, 0), "scroll_down", (0, 53, 0, 176)),
    "up-empty-output": ((400, 0), (53, 176), (0, 0), "scroll_up", (0, 53, 0, 176)),
    "up-from-past-last-start": ((276, 0), (53, 176), (300, 0), "scroll_up", (196, 249, 0, 176)),
    "bottom-from-top": ((0, 0), (30, 120), (200, 200), "scroll_bottom", (171, 201, 0, 120)),
    "bottom-from-middle": ((40, 50), (30, 120), (200, 200), "scroll_bottom", (171, 201, 50, 170)),
    "bottom-at-bottom": ((171, 50), (30, 120), (200, 200), "scroll_bottom", (171, 201, 50, 170)),
    "reset-to-origin": ((400, 500), (53, 176), (500, 500), "reset_display", (0, 53, 0, 176)),
}


@pytest.mark.parametrize("start, term, output, action, expected", SCROLLS.values(), ids=SCROLLS.keys())
def test_one_scroll_moves_the_window_as_expected(start, term, output, action, expected):
    viewport = Viewport(*start)
    viewport.set_term_size(*term)
    viewport.max_height, viewport.max_width = output
    getattr(viewport, action)()
    window = (viewport.v_start, viewport.v_stop, viewport.h_start, viewport.h_stop)
    assert window == expected
    assert all(isinstance(edge, int) for edge in window)
    assert (viewport.max_height, viewport.max_width) == output  # scrolling never changes the output size


def test_after_double_scroll_down():
    viewport = Viewport()
    viewport.set_term_size(50, 120)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (49, 99) == (viewport.v_start, viewport.v_stop)  # one page: 49 lines, the last row is for notices
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (98, 148) == (viewport.v_start, viewport.v_stop)
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (147, 197) == (viewport.v_start, viewport.v_stop)


def test_after_scroll_down_scroll_up():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_height = 300
    viewport.scroll_down()
    assert 300 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 52 == viewport.v_start  # one page of 52 lines
    assert 105 == viewport.v_stop

    viewport.scroll_up()  # Now we can scroll up again

    assert 300 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_pages_miss_no_line(tmp_path):  # qtop/qtop#121: a line went missing between pages
    from qtop_py.ui.mvu_view import TextDisplay

    source = tmp_path / "frame.out"
    source.write_text("".join("%d\n" % number for number in range(100)))
    display = TextDisplay.__new__(TextDisplay)
    display.config = {"savepath": str(tmp_path)}
    viewport = Viewport()
    viewport.set_term_size(10, 80)
    viewport.max_height = 100
    shown = []
    while True:
        shown += Path(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read_text().split()
        if not viewport.scroll_down():
            break
    assert sorted(set(int(line) for line in shown)) == list(range(100))  # the last page may overlap, to fill the screen
    viewport.scroll_bottom()
    last_page = Path(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read_text()
    assert last_page.split()[-1] == "99"


@pytest.mark.parametrize(
    "text,columns,expected",
    [
        ("abcd\n", 4, ["abcd"]),
        ("abcde\n", 4, ["abcd", "e"]),
        ("abcd\nefgh\n", 4, ["abcd", "efgh"]),
        ("\n", 4, [""]),
        ("\rheader\n", 4, ["head", "er"]),
        ("a\tb\n", 4, ["a   ", "    ", "b"]),
        ("\u754c\u754cx\n", 4, ["\u754c\u754c", "x"]),
        ("e\u0301xy\n", 2, ["e\u0301x", "y"]),
        ("\u754c\n", 1, ["?"]),
    ],
    ids=("exact-width", "overflow", "exact-newlines", "blank", "banner-return", "tab", "wide-glyphs", "combining", "one-column"),
)
def test_terminal_rows_fit_visible_cells(text, columns, expected):
    assert list(terminal_rows(text.splitlines(keepends=True) if not text.startswith("\r") else [text], columns)) == [row + "\n" for row in expected]


@pytest.mark.parametrize("columns", (4, 8, 16), ids=("very-narrow", "narrow", "wide"))
def test_wrapped_viewport_keeps_top_message_and_every_scrollable_row(tmp_path, columns):
    from qtop_py.qtop import get_output_size
    from qtop_py.ui.mvu_view import TextDisplay

    source = tmp_path / "frame.out"
    text = "TOP\n\x1b[31mABCDEFGHIJKLMNOPQRSTUVWXYZ\x1b[0m\nlast\n"
    source.write_text(text)
    viewport = Viewport()
    viewport.set_term_size(4, columns)
    viewport.max_height = get_output_size(0, str(source), term_columns=columns)[0]
    display = TextDisplay.__new__(TextDisplay)
    display.config, display.viewport = {"savepath": str(tmp_path)}, viewport
    rows = {}
    while True:
        page = Path(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read_text()
        plain = re.sub(r"\x1b\[[0-9;]*m", "", page).splitlines()
        assert len(plain) <= viewport.page
        assert all(len(row) <= columns for row in plain)
        if not rows:
            assert plain[0] == "TOP"
        rows.update((viewport.v_start + index, row) for index, row in enumerate(plain))
        if not viewport.scroll_down():
            break
    assert len(rows) == viewport.max_height
    assert "".join(rows[index] for index in sorted(rows)) == "TOPABCDEFGHIJKLMNOPQRSTUVWXYZlast"
    viewport.scroll_bottom()
    assert "last" in Path(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read_text()


def test_wrapped_rows_restore_ansi_style_on_continuations_and_new_pages():
    rows = list(terminal_rows(["\x1b[31mabcdef\n", "gh\x1b[0m\n"], 3))
    assert rows == ["\x1b[31mabc\x1b[0m\n", "\x1b[31mdef\x1b[0m\n", "\x1b[31mgh\x1b[0m\n"]


def test_unbounded_rows_preserve_original_output():
    assert list(terminal_rows(["abc\n", "def"], 0)) == ["abc\n", "def"]


def test_terminal_reset_is_zero_width_and_does_not_carry_an_old_style():
    rows = list(terminal_rows(["\x1b[31ma\n", "\x1bcabcd\n"], 4))
    assert rows == ["\x1b[31ma\x1b[0m\n", "\x1b[31m\x1bcabcd\n"]


@pytest.mark.parametrize("hidden_rows", (0, 140, 290))
def test_a_shorter_output_keeps_the_view_inside_it(hidden_rows):
    """Scrolled down, then 2 hides the node matrix: the view must not start past the new end."""
    viewport = Viewport()
    viewport.set_term_size(50, 120)
    viewport.max_width, viewport.max_height = 120, 300
    viewport.scroll_bottom()
    assert viewport.v_start == 300 - 49
    viewport.max_height = 300 - hidden_rows
    assert viewport.v_start == max(300 - hidden_rows - 49, 0)
    assert viewport.v_start < max(viewport.max_height, 1)  # something is always on screen


def test_a_narrower_output_and_a_taller_terminal_keep_the_view_inside():
    viewport = Viewport()
    viewport.set_term_size(20, 80)
    viewport.max_width, viewport.max_height = 400, 100
    viewport.scroll_far_right()
    viewport.scroll_bottom()
    viewport.max_width = 100
    assert viewport.h_start == 20
    viewport.set_term_size(60, 80)
    assert viewport.v_start == 100 - 59
