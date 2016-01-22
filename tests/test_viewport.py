import pytest
from ui.viewport import Viewport


class TestViewport:

    # def setup_class(cls):
    #     viewport = Viewport()

    def setup(self):
        self.viewport = Viewport()
        self.viewport.reset_term_size(53, 176)

    def test_reset_display(self, monkeypatch):
        self.viewport.reset_display()
        assert self.viewport.get_config('v_stop') == self.viewport.v_stop

    def test_scroll_right(self):
        self.viewport.reset_display()
        self.viewport.scroll_right()
        self.viewport.h_offset = self.viewport.h_stop - self.viewport.h_start
        assert self.viewport.h_offset == self.viewport.get_h_term_size()

    # def test_scroll_bottom(self):
    #     self.viewport.reset_display()
    #     self.viewport.scroll_bottom()
    #     assert False
    #
    # def test_scroll_bottom_fails(self):
    #     self.viewport.reset_display()
    #     assert False

    def test_reset_display(self):
        self.viewport.reset_display()
        assert self.viewport.v_start == 1
        assert self.viewport.h_start == 0

    def test_scroll_left(self):
        self.viewport.reset_display()
        self.viewport.scroll_left()
        assert self.viewport.v_start == 1
        assert self.viewport.h_start == 0

    # def test_scroll_far_right(self):
    #     self.viewport.reset_display()
    #     self.viewport.scroll_far_right()
    #     assert self.viewport.v_stop == self.viewport.get_v_stop
    #     assert self.viewport.h_stop == self.viewport.get_h_stop
