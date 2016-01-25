class Viewport(object):

    def __init__(self, hstart=0, vstart=0):
        self._v_start = vstart
        self._v_stop = 0
        self._h_start = hstart
        self._h_stop = 0
        self.max_height = 0  # zero until a non-empty qtop output file is read
        self.max_width = 0
        self.term_size = [0, 0]

    @property
    def h_start(self):
        if self._h_start < 0:
            self._h_start = 0
        return self._h_start

    @property
    def v_start(self):
        if self._v_start < 0:
            self._v_start = 0
        return self._v_start

    @property
    def h_stop(self):
        return self._h_start + self.get_h_term_size()

    @property
    def v_stop(self):
        return self._v_start + self.get_v_term_size()

    @v_start.setter
    def v_start(self, value):
        if value > self.max_height -self.get_v_term_size():
            self._v_start = self.max_height - self.get_v_term_size()
        elif value < 0:
            self._v_start = 0
        else:
            self._v_start = value

    @h_start.setter
    def h_start(self, value):
        if value > self.max_width - self.get_h_term_size():
            self._h_start = self.max_width - self.get_h_term_size()
        elif value < 0:
            self._h_start = 0
        else:
            self._h_start = value

    def set_term_size(self, term_height, term_columns):
        self.term_size = [term_height, term_columns]

    def get_v_term_size(self):
        return self.term_size[0]

    def get_h_term_size(self):
        return self.term_size[1]

    def set_max_width(self, max_width):
        self.max_width = max_width

    def set_max_height(self, max_height):
        self.max_height = max_height

    def get_max_height(self):
        return self.max_height

    def scroll_down(self):
        success = False
        if self.v_stop < self.max_height:
            self.v_start += self.get_v_term_size()
            success = True
        return success

    def scroll_bottom(self):
        self.v_start = self.max_height - self.get_v_term_size()
        return True

    def scroll_up(self):
        # This can't get a negative value because of its property setup
        self.v_start -= self.get_v_term_size()
        return True

    def scroll_top(self):
        self.v_start = 0
        return True

    def scroll_right(self):
        self.h_start += self.get_h_term_size()/2
        return True

    def scroll_far_right(self):
        self.h_start = self.max_width - self.get_h_term_size()

    def scroll_left(self):
        self.h_start -= self.get_h_term_size()/2

    def scroll_far_left(self):
        self.h_start = 0

    def reset_display(self):
        self.v_start = 0
        self.h_start = 0
