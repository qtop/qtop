import pytest
from qtop_py.utils import ColorStr, CountCalls


class TestColorStr:
    def test_init_default(self):
        cs = ColorStr()
        assert cs.str == ""
        assert cs.color == ""
        assert cs.initial == ""
        assert cs.index == 0
        assert cs.stop == 0

    def test_init_with_values(self):
        cs = ColorStr(string="hello", color="red")
        assert cs.str == "hello"
        assert cs.color == "red"
        assert cs.initial == "h"
        assert cs.index == 0
        assert cs.stop == 5

    def test_str(self):
        cs = ColorStr(string="world")
        assert str(cs) == "world"

    def test_repr(self):
        cs = ColorStr(string="test")
        assert repr(cs) == "'test'"

    def test_len(self):
        cs = ColorStr(string="abc")
        assert len(cs) == 1  # returns len(initial)

    def test_contains(self):
        cs = ColorStr(string="hello")
        assert "he" in cs
        assert "x" not in cs

    def test_from_other_color_str(self):
        cs1 = ColorStr(string="original", color="blue")
        cs2 = ColorStr.from_other_color_str(cs1)
        assert cs2.str == "original"
        assert cs2.color == ""  # from_other_color_str does not copy color
        assert cs2.initial == "o"


class TestCountCalls:
    def test_counts_calls(self):
        @CountCalls
        def my_func():
            return 42

        assert my_func() == 42
        assert my_func() == 42
        assert my_func.count() == 2

    def test_static_counts_method(self):
        @CountCalls
        def func_a():
            pass

        @CountCalls
        def func_b():
            pass

        func_a()
        func_a()
        func_b()

        counts = CountCalls.counts()
        assert counts["func_a"] == 2
        assert counts["func_b"] == 1
