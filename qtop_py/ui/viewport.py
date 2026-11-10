## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The scrollable window onto qtop's rendered output."""

from typing import Tuple


class BaseViewport(object):
    """Class that implements a basic viewport [window into the qtop matrix] and basic movement restrictions."""

    def __init__(self, vstart: int = 0, hstart: int = 0) -> None:
        self._v_start = vstart
        self._v_stop = 0
        self._h_start = hstart
        self._h_stop = 0
        self.max_height = 0  # zero until a non-empty qtop output file is read
        self.max_width = 0
        self.v_term_size = 0
        self.h_term_size = 0

    def get_up_limit(self) -> int:
        """Return the top limit, 0."""
        return 0

    def get_right_limit(self) -> int:
        """Clamp empty or narrower-than-terminal output to the left edge."""
        return max(self.max_width - self.h_term_size, 0)

    @property
    def page(self) -> int:
        """Lines one screen shows: the terminal's last row is kept for qtop's notices."""
        return max(self.v_term_size - 1, 1)

    def get_down_limit(self) -> int:
        """Clamp empty or shorter-than-terminal output to the top edge."""
        return max(self.max_height - self.page, 0)

    def get_left_limit(self) -> int:
        """Return the left limit, 0."""
        return 0

    def would_cross_up_limit(self, value: int) -> bool:
        """Return True if value is above the top limit."""
        return value < self.get_up_limit()

    def would_cross_right_limit(self, value: int) -> bool:
        """Return True if value is beyond the right limit."""
        return value > self.get_right_limit()

    def would_cross_down_limit(self, value: int) -> bool:
        """Return True if value is below the bottom limit."""
        return value > self.get_down_limit()

    def would_cross_left_limit(self, value: int) -> bool:
        """Return True if value is left of the left limit."""
        return value < self.get_left_limit()


class Viewport(BaseViewport):
    """Class that extends BaseViewport to provide the actual movements of the Viewport."""

    @property
    def h_start(self) -> int:
        """The first visible column."""
        return self._h_start

    @h_start.setter
    def h_start(self, value: int) -> None:
        """The first visible column."""
        if self.would_cross_right_limit(value):
            self._h_start = self.get_right_limit()
        elif self.would_cross_left_limit(value):
            self._h_start = self.get_left_limit()
        else:
            self._h_start = value

    @property
    def v_start(self) -> int:
        """The first visible line."""
        return self._v_start

    @v_start.setter
    def v_start(self, value: int) -> None:
        """The first visible line."""
        if self.would_cross_up_limit(value):
            self._v_start = self.get_up_limit()
        elif self.would_cross_down_limit(value):
            self._v_start = self.get_down_limit()
        else:
            self._v_start = value

    @property
    def h_stop(self) -> int:
        """The column after the last visible one."""
        return self._h_start + self.h_term_size

    @property
    def v_stop(self) -> int:
        """The line after the last visible one."""
        return self._v_start + self.v_term_size

    def set_term_size(self, term_height: int, term_columns: int) -> None:
        """Set the terminal size in lines and columns."""
        self.v_term_size = term_height
        self.h_term_size = term_columns

    def get_term_size(self) -> Tuple[int, int]:
        """Return the terminal size as (lines, columns)."""
        return self.v_term_size, self.h_term_size

    def scroll_down(self) -> bool:
        """Scroll down one page; return False at the end."""
        success = False
        if self.v_start + self.page < self.max_height:
            self.v_start += self.page
            success = True
        return success

    def scroll_bottom(self) -> bool:
        """Scroll to the last page; return False if already there."""
        success = False
        if self.v_start + self.page < self.max_height:
            self.v_start = self.get_down_limit()
            success = True
        return success

    def scroll_up(self) -> bool:
        """Scroll up one page."""
        self.v_start -= self.page
        return True

    def scroll_top(self) -> bool:
        """Scroll to the top."""
        self.v_start = 0
        return True

    def scroll_right(self) -> bool:
        """Scroll right half a screen; return False at the right edge."""
        if self.h_start + self.h_term_size >= self.max_width:
            return False
        self.h_start += self.h_term_size // 2
        return True

    def scroll_far_right(self) -> None:
        """Scroll to the right edge."""
        self.h_start = self.get_right_limit()

    def scroll_left(self) -> None:
        """Scroll left half a screen."""
        self.h_start -= self.h_term_size // 2

    def scroll_far_left(self) -> None:
        """Scroll to the left edge."""
        self.h_start = 0

    def reset_display(self) -> None:
        """Scroll back to the top-left corner."""
        self.v_start = 0
        self.h_start = 0
