from qtop_py.ui.viewport import Viewport
import pytest


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
    assert 176 == viewport.h_stop  # BUG?? or assert(10 == viewport.h_stop) is a BUG
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_right():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_right()
    assert 176 / 2 == viewport.h_start  # corrected behaviour: last element should touch right screen edge, if possible!
    assert 176 / 2 + 176 == viewport.h_stop  # (not scroll endelessly to the right)
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_down():
    viewport = Viewport()
    viewport.set_term_size(50, 120)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (50, 100) == (viewport.v_start, viewport.v_stop)


def test_after_double_scroll_down():
    viewport = Viewport()
    viewport.set_term_size(50, 120)
    viewport.max_width = 400
    viewport.max_height = 200
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (50, 100) == (viewport.v_start, viewport.v_stop)
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (100, 150) == (viewport.v_start, viewport.v_stop)
    viewport.scroll_down()
    assert (0, 120) == (viewport.h_start, viewport.h_stop)
    assert (150, 200) == (viewport.v_start, viewport.v_stop)


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
    assert 53 == viewport.v_stop  # BUG? Should this change now? Why was it 50 before? Did anything really change?


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
    assert 276 - 53 == viewport.v_start
    assert 276 == viewport.v_stop


def test_after_scroll_down_scroll_up():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.max_height = 300
    viewport.scroll_down()
    assert 300 == viewport.max_height
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 53 == viewport.v_start  # This scrolling logic looks ok
    assert 106 == viewport.v_stop

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
@pytest.mark.parametrize('init_vstart, init_hstart, term_size, max_matrix_dim, expected',
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


@pytest.mark.parametrize('init_vstart, init_hstart, term_size, max_matrix_dim, expected',
     (
         (0, 0, [30, 120], (200, 200), (170, 200, 0, 120)),  # from top to bottom
         (40, 50, [30, 120], (200, 200), (170, 200, 50, 170)),  # from random to bottom
         (170, 50, [30, 120], (200, 200), (170, 200, 50, 170)),  # from bottom to bottom
     ),
)
def test_after_scroll_bottom(init_vstart, init_hstart, term_size, max_matrix_dim, expected):
    viewport = Viewport(init_vstart, init_hstart)
    viewport.set_term_size(*term_size)
    viewport.max_width = max_matrix_dim[1]
    viewport.max_height = max_matrix_dim[0]
    viewport.scroll_bottom()
    assert expected == (viewport.v_start, viewport.v_stop, viewport.h_start, viewport.h_stop)
