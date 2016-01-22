from ui.viewport import Viewport

class DefaultProvider(object):

    def __init__(self, h_stop, v_stop):
        self._conf = {
            'h_stop': h_stop,
            'v_stop': v_stop
        }

    def get_config(self, config):
        return self._conf[config]


def test_defaults():
    viewport = Viewport(None)
    assert(0 == viewport.get_h_start())
    assert(viewport.get_h_stop() is None)
    assert(1 == viewport.get_v_start())
    assert(viewport.get_v_stop() is None)


def test_after_reset():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())
    assert(50 == viewport.get_v_stop())


def test_after_reset_from_term():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())
    assert(50 == viewport.get_v_stop())


def test_after_scroll_left():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    viewport.scroll_left()
    assert(0 == viewport.get_h_start())
    assert(300 == viewport.get_h_stop())  # BUG?? or assert(10 == viewport.get_h_stop()) is a BUG
    assert(1 == viewport.get_v_start())
    assert(50 == viewport.get_v_stop())


def test_after_scroll_right():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    viewport.scroll_right()
    assert(150 == viewport.get_h_start())  # BUG?! Where did this 150 get from? Half a term size - I guess
    assert(160 == viewport.get_h_stop())   # But even in that case - I don't see why 160 is remaining
    assert(1 == viewport.get_v_start())    # ... actually it's because its previous "10 == viewport.get_h_stop()" and
    assert(50 == viewport.get_v_stop())    # We add half a screen to both. To be confirmed that it makes any sense


def test_after_scroll_up_no_num_lines():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    assert(not viewport.scroll_up())
    assert(0 == viewport.get_num_lines())
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())  # Looks good - didn't change
    assert(40 == viewport.get_v_stop())  # BUG? Should this change now? Why was it 50 before? Did anything really change?


def test_after_scroll_down_no_num_lines():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    assert(not viewport.scroll_down())
    assert(0 == viewport.get_num_lines())
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())  # BUG? This still didn't change (!!)
    assert(50 == viewport.get_v_stop())  # Why this becomes 50...


def test_after_scroll_up():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    viewport.set_num_lines(100)
    rv = viewport.scroll_up()
    assert not rv
    assert(100 == viewport.get_num_lines())
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())  # Ok in the sense that it didn't change
    assert(40 == viewport.get_v_stop())


def test_after_scroll_down():
    dp = DefaultProvider(10, 50)
    viewport = Viewport(dp)
    viewport.reset_stops_from_config()
    viewport.reset_term_size(40, 300)
    viewport.set_num_lines(100)
    rv = viewport.scroll_down()
    assert rv is None  # Bug, should return True on success
    assert(100 == viewport.get_num_lines())
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(41 == viewport.get_v_start())  # This scrolling logic looks ok
    assert(90 == viewport.get_v_stop())

    # Now we can scroll up
    rv = viewport.scroll_up()
    assert rv

    assert(100 == viewport.get_num_lines())
    assert(0 == viewport.get_h_start())
    assert(10 == viewport.get_h_stop())
    assert(1 == viewport.get_v_start())  # This scrolling logic looks ok
    assert(50 == viewport.get_v_stop())
