

class Viewport(object):

    def __init__(self, limits_provider):
        self.v_start = 1
        self.v_stop = None
        self.h_start = 0
        self.h_stop = None
        self.limits_provider = limits_provider
        self.num_lines = 0
        self.max_full_line_len = None
        self.config = {}

    def reset_stops_from_config(self):
        if self.h_stop is None:
            self.h_stop = self.limits_provider.get_config('h_stop')
        if self.v_stop is None:
            self.v_stop = self.limits_provider.get_config('v_stop')

    def set_max_full_line_len(self, max_full_line_len):
        self.max_full_line_len = max_full_line_len

    def set_num_lines(self, num_lines):
        self.num_lines = num_lines

    def get_num_lines(self):
        return self.num_lines

    def get_v_start(self):
        return self.v_start

    def get_v_stop(self):
        return self.v_stop

    def get_h_start(self):
        return self.h_start

    def get_h_stop(self):
        return self.h_stop

    def init_from_config(self, config):
        for key in ['h_start', 'h_stop', 'v_stop', 'term_size']:
            self.config[key] = config.get(key, None)

    def get_v_term_size(self):
        return self.config['term_size'][0]

    def get_h_term_size(self):
        return self.config['term_size'][1]

    def set_config(self, attribute, value):
        assert attribute in ['h_start', 'h_stop', 'v_stop']
        self.config[attribute] = value

    def get_config(self, attribute):
        assert attribute in ['h_start', 'h_stop', 'v_stop']
        return self.config[attribute]

    def reset_term_size(self, term_height, term_columns):
        self.config['term_size'] = [term_height, term_columns]
        self.config['h_start'] = self.h_start
        self.config['v_stop'], self.config['h_stop'] = self.config['term_size']

    def scroll_down(self):

        if self.v_stop < self.num_lines:

            self.v_start += self.get_v_term_size()
            self.v_stop += self.get_v_term_size()  # - 10

            self.set_config('v_stop', self.v_stop)
        else:
            return False

    def scroll_bottom(self):
        self.v_start = self.num_lines - self.get_v_term_size()
        self.v_stop = self.num_lines

        self.set_config('v_stop', self.v_stop)

    def scroll_top(self):
        self.v_start = 0
        self.v_stop = self.get_v_term_size()

        self.set_config('v_stop', self.v_stop)

    def scroll_up(self):
        success = False
        if self.v_start - self.get_v_term_size() >= 0:
            self.v_start -= self.get_v_term_size()
            self.v_stop -= self.get_v_term_size()
            success = True
        else:
            self.v_start = 1
            self.v_stop = self.get_v_term_size()

        self.set_config('v_stop', self.v_stop)
        return success

    def scroll_right(self):  # 'l', right
        self.h_start += self.get_h_term_size()/2
        self.h_stop += self.get_h_term_size()/2

        self.set_config('h_start', self.h_start)
        self.set_config('h_stop', self.h_stop)

    def scroll_far_right(self):  # 'l', right
        self.h_start = self.max_full_line_len - self.get_h_term_size()
        self.h_stop = self.max_full_line_len

        self.set_config('h_start', self.h_start)
        self.set_config('h_stop', self.h_stop)

    def scroll_left(self):
        if self.h_start >= self.get_h_term_size() / 2:
            self.h_start -= self.get_h_term_size()/2
        if self.h_start > 0:
            self.h_stop -= self.get_h_term_size()/2
        else:
            self.h_stop = self.get_h_term_size()

        self.set_config('h_start', self.h_start)
        self.set_config('h_stop', self.h_stop)

    def scroll_far_left(self):
        self.h_start = 0
        self.h_stop = self.get_h_term_size()

        self.set_config('h_start', self.h_start)
        self.set_config('h_stop', self.h_stop)

    def reset_display(self):  # "R", reset display
        self.v_start = 1
        self.v_stop = self.get_v_term_size()
        self.h_start = 0
        self.h_stop = self.get_h_term_size()

        self.set_config('v_stop', self.v_stop)
