## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import pytest

from qtop_py.ui.viewport import Viewport


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


def test_after_scroll_left():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_width = 200
    viewport.max_height = 200
    viewport.scroll_left()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_right_moves_half_a_page():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_right()
    assert 176 // 2 == viewport.h_start
    assert 176 // 2 + 176 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop
    assert isinstance(viewport.h_start, int)
    assert isinstance(viewport.h_stop, int)


def test_after_scroll_down():
    viewport = Viewport()
    viewport.set_term_size(50, 120)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (49, 99) == (viewport.v_start, viewport.v_stop)  # one page: 49 lines, the last row is for notices


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


def test_after_scroll_right_with_nowhere_to_go():
    viewport = Viewport(hstart=400 - 176)
    viewport.set_term_size(53, 176)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_right()
    assert 400 - 176 == viewport.h_start  # OK, shouldn't move!
    assert 400 == viewport.h_stop  # (not scroll endelessly to the right)
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_right_when_not_all_space_available_to_the_right():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_width = 200
    viewport.max_height = 200
    viewport.scroll_right()
    assert 200 - 176 == viewport.h_start  # corrected behaviour: last element should touch right screen edge, if possible!
    assert 200 == viewport.h_stop  # (not scroll endelessly to the right)
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_up_no_max_height():
    viewport = Viewport(vstart=400)
    viewport.set_term_size(53, 176)
    viewport.scroll_up()
    assert 0 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start  # Looks good - didn't change
    assert 53 == viewport.v_stop


def test_after_scroll_down_no_max_height():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.scroll_down()
    assert 0 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start  # This shouldn't change if there's no maxheight (=empty file)
    assert 53 == viewport.v_stop  # taking up the space of the designated screen


def test_after_scroll_up():
    viewport = Viewport(vstart=276)
    viewport.set_term_size(53, 176)
    viewport.max_height = 300
    viewport.scroll_up()
    assert 300 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 276 - 52 == viewport.v_start
    assert 276 - 52 + 53 == viewport.v_stop


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


def test_after_reset_to_starting_position():
    viewport = Viewport(400, 500)
    viewport.set_term_size(53, 176)
    viewport.max_width = 500
    viewport.max_height = 500
    viewport.reset_display()
    assert 0 == viewport.v_start
    assert 0 == viewport.h_start
    assert 53 == viewport.v_stop
    assert 176 == viewport.h_stop


def test_scroll_far_right_attaches_to_right_screen_edge():
    viewport = Viewport(200, 200)
    viewport.set_term_size(53, 176)
    viewport.max_width = 400
    viewport.max_height = 400
    viewport.scroll_far_right()
    assert 400 - 176 == viewport.h_start
    assert 400 == viewport.h_stop
    assert 200 == viewport.v_start
    assert 200 + 53 == viewport.v_stop


"""
This is a quick'n'clean way to run many edge cases without re-writing the whole bloody initialisation every time!!
"""


@pytest.mark.parametrize(
    "init_vstart, init_hstart, term_size, max_matrix_dim, expected",
    (
        (0, 0, [53, 176], (200, 200), (0, 53, 24, 200)),  # test1
        (0, 100, [53, 176], (400, 400), (0, 53, 188, 364)),  # test2 etc
        (0, 100, [53, 176], (400, 200), (0, 53, 100, 276)),
        (0, 199, [53, 176], (400, 200), (0, 53, 199, 375)),
    ),
)
def test_after_scroll_right(init_vstart, init_hstart, term_size, max_matrix_dim, expected):
    viewport = Viewport(init_vstart, init_hstart)
    viewport.set_term_size(*term_size)
    viewport.max_width = max_matrix_dim[1]
    viewport.max_height = max_matrix_dim[0]
    viewport.scroll_right()
    # corrected behaviour: last element should touch right screen edge, if possible!
    assert expected == (viewport.v_start, viewport.v_stop, viewport.h_start, viewport.h_stop)


@pytest.mark.parametrize(
    "init_vstart, init_hstart, term_size, max_matrix_dim, expected",
    (
        (0, 0, [30, 120], (200, 200), (171, 201, 0, 120)),  # from top to bottom: the last 29-line page
        (40, 50, [30, 120], (200, 200), (171, 201, 50, 170)),  # from random to bottom
        (171, 50, [30, 120], (200, 200), (171, 201, 50, 170)),  # from bottom to bottom
    ),
)
def test_after_scroll_bottom(init_vstart, init_hstart, term_size, max_matrix_dim, expected):
    viewport = Viewport(init_vstart, init_hstart)
    viewport.set_term_size(*term_size)
    viewport.max_width = max_matrix_dim[1]
    viewport.max_height = max_matrix_dim[0]
    viewport.scroll_bottom()
    assert expected == (viewport.v_start, viewport.v_stop, viewport.h_start, viewport.h_stop)


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
        shown += open(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read().split()
        if not viewport.scroll_down():
            break
    assert sorted(set(int(line) for line in shown)) == list(range(100))  # the last page may overlap, to fill the screen
    viewport.scroll_bottom()
    assert open(display.show_part_view("t", str(source), viewport.v_start, viewport.v_term_size)).read().split()[-1] == "99"
