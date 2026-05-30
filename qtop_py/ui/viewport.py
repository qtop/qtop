class BaseViewport(object):
    """
    Class that implements a basic viewport [window into the qtop matrix] and basic movement restrictions
    """
    def __init__(self, vstart=0, hstart=0):
        self._v_start = vstart
        self._v_stop = 0
        self._h_start = hstart
        self._h_stop = 0
        self.max_height = 0  # zero until a non-empty qtop output file is read
        self.max_width = 0
        self.v_term_size = 0
        self.h_term_size = 0

    def get_up_limit(self):
        return 0

    def get_right_limit(self):
        # TODO check validity of this: extreme case for no initial max_width, i.e. no matrix output
        return max(self.max_width - self.h_term_size, 0)

    def get_down_limit(self):
        # TODO check validity of this: extreme case for no initial max_height, i.e. no matrix output
        return max(self.max_height - self.v_term_size, 0)

    def get_left_limit(self):
        return 0

    def would_cross_up_limit(self, value):
        return value < self.get_up_limit()

    def would_cross_right_limit(self, value):
        return value > self.get_right_limit()

    def would_cross_down_limit(self, value):
        return value > self.get_down_limit()

    def would_cross_left_limit(self, value):
        return value < self.get_left_limit()


class Viewport(BaseViewport):
    """
    Class that extends BaseViewport to provide the actual movements of the Viewport
    """
    @property
    def h_start(self):
        assert not (self.would_cross_left_limit(self._h_start) and self.would_cross_right_limit(self._h_start))
        return self._h_start

    @h_start.setter
    def h_start(self, value):
        if self.would_cross_right_limit(value):
            self._h_start = self.get_right_limit()
        elif self.would_cross_left_limit(value):
            self._h_start = self.get_left_limit()
        else:
            self._h_start = value

    @property
    def v_start(self):
        assert not (self.would_cross_up_limit(self._v_start) and self.would_cross_down_limit(self._v_start))
        return self._v_start

    @v_start.setter
    def v_start(self, value):
        if self.would_cross_up_limit(value):
            self._v_start = self.get_up_limit()
        elif self.would_cross_down_limit(value):
            self._v_start = self.get_down_limit()
        else:
            self._v_start = value

    @property
    def h_stop(self):
        return self._h_start + self.h_term_size

    @property
    def v_stop(self):
        return self._v_start + self.v_term_size

    def set_term_size(self, term_height, term_columns):
        self.v_term_size = term_height
        self.h_term_size = term_columns

    def get_term_size(self):
        return self.v_term_size, self.h_term_size

    def scroll_down(self):
        success = False
        if self.v_stop < self.max_height:
            self.v_start += self.v_term_size
            success = True
        return success

    def scroll_bottom(self):
        success = False
        if self.v_stop < self.max_height:
            self.v_start = self.get_down_limit()
            success = True
        return success

    def scroll_up(self):
        self.v_start -= self.v_term_size
        return True

    def scroll_top(self):
        self.v_start = 0
        return True

    def scroll_right(self):
        if self.h_start + self.h_term_size >= self.max_width:
            return False
        self.h_start += self.h_term_size / 2
        return True

    def scroll_far_right(self):
        self.h_start = self.get_right_limit()

    def scroll_left(self):
        self.h_start -= self.h_term_size / 2

    def scroll_far_left(self):
        self.h_start = 0

    def reset_display(self):
        self.v_start = 0
        self.h_start = 0
