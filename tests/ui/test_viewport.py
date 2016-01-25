from ui.viewport import Viewport


def test_defaults():
    viewport = Viewport()
    assert 0 == viewport.h_start
    assert 0 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 0 == viewport.v_stop


def test_after_set_term_size():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    assert 53 == viewport.get_v_term_size()
    assert 176 == viewport.get_h_term_size()


def test_after_scroll_left():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.scroll_left()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop  # BUG?? or assert(10 == viewport.h_stop) is a BUG
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_right():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.set_max_width(200)
    viewport.set_max_height(200)
    viewport.scroll_right()
    assert 24 == viewport.h_start  # corrected behaviour: last element should touch right screen edge, if possible!
    assert 200 == viewport.h_stop  # (not scroll endelessly to the right)
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop


def test_after_scroll_up_no_max_height():
    viewport = Viewport(vstart=400)
    viewport.set_term_size(53, 176)
    viewport.scroll_up()
    assert 0 == viewport.get_max_height()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start  # Looks good - didn't change
    assert 53 == viewport.v_stop  # BUG? Should this change now? Why was it 50 before? Did anything really change?


def test_after_scroll_down_no_max_height():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.scroll_down()
    assert 0 == viewport.get_max_height()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start  # This shouldn't change if there's no maxheight (=empty file)
    assert 53 == viewport.v_stop  # taking up the space of the designated screen


def test_after_scroll_up():
    viewport = Viewport(vstart=276)
    viewport.set_term_size(53, 176)
    viewport.set_max_height(300)
    viewport.scroll_up()
    assert 300 == viewport.get_max_height()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 276 - 53 == viewport.v_start
    assert 276 == viewport.v_stop


def test_after_scroll_down_scroll_up():
    viewport = Viewport()
    viewport.set_term_size(53, 176)
    viewport.set_max_height(300)
    viewport.scroll_down()
    assert 300 == viewport.get_max_height()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 53 == viewport.v_start  # This scrolling logic looks ok
    assert 106 == viewport.v_stop

    # Now we can scroll up again
    viewport.scroll_up()

    assert 300 == viewport.get_max_height()
    assert 0 == viewport.h_start
    assert 176 == viewport.h_stop
    assert 0 == viewport.v_start
    assert 53 == viewport.v_stop
