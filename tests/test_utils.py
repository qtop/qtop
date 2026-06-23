##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import sys
from qtop_py.utils import ColorStr, CountCalls, parse_qtop_cmdline_args


class TestColorStr:
    def test_str_returns_string_value(self):
        cs = ColorStr("hello", "Red")
        assert str(cs) == "hello"

    def test_repr_returns_repr(self):
        cs = ColorStr("hello", "Red")
        assert repr(cs) == repr("hello")

    def test_len_returns_initial_char_length(self):
        cs = ColorStr("hello", "Red")
        assert len(cs) == 1

    def test_len_empty_string(self):
        cs = ColorStr("", "")
        assert len(cs) == 0

    def test_initial_set_to_first_char(self):
        cs = ColorStr("abc", "Blue")
        assert cs.initial == "a"

    def test_initial_empty_string(self):
        cs = ColorStr("", "")
        assert cs.initial == ""

    def test_contains_true(self):
        cs = ColorStr("hello world", "Green")
        assert "world" in cs

    def test_contains_false(self):
        cs = ColorStr("hello", "Green")
        assert "xyz" not in cs

    def test_from_other_color_str(self):
        original = ColorStr("data", "Red")
        copy = ColorStr.from_other_color_str(original)
        assert str(copy) == "data"
        assert copy.color == ""

    def test_iterator_protocol_not_implemented(self):
        cs = ColorStr("ab", "Cyan")
        assert not hasattr(cs, "__next__")

    def test_next_method_exists(self):
        cs = ColorStr("a", "Red")
        assert hasattr(cs, "next")


class TestCountCalls:
    def test_counts_function_calls(self):
        @CountCalls
        def add(a, b):
            return a + b

        assert add.count() == 0
        add(1, 2)
        assert add.count() == 1
        add(3, 4)
        assert add.count() == 2

    def test_counts_multiple_functions(self):
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

    def test_return_value_preserved(self):
        @CountCalls
        def multiply(x, y):
            return x * y

        result = multiply(3, 4)
        assert result == 12


class TestParseQtopCmdlineArgs:
    def test_default_args(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop"])
        args = parse_qtop_cmdline_args()
        assert args.BATCH_SYSTEM is None
        assert args.COLOR == "AUTO"
        assert args.CLASSIC is False
        assert args.DEBUG is False
        assert args.EXPORT is False

    def test_batch_system_flag(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-b", "pbs"])
        args = parse_qtop_cmdline_args()
        assert args.BATCH_SYSTEM == "pbs"

    def test_color_on(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-c", "ON"])
        args = parse_qtop_cmdline_args()
        assert args.COLOR == "ON"

    def test_color_off(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-c", "OFF"])
        args = parse_qtop_cmdline_args()
        assert args.COLOR == "OFF"

    def test_classic_mode(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-C"])
        args = parse_qtop_cmdline_args()
        assert args.CLASSIC is True

    def test_debug_mode(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-d"])
        args = parse_qtop_cmdline_args()
        assert args.DEBUG is True

    def test_export_flag(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-E"])
        args = parse_qtop_cmdline_args()
        assert args.EXPORT is True

    def test_disable_section1(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-1"])
        args = parse_qtop_cmdline_args()
        assert args.sect_1_off is True

    def test_disable_section2(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-2"])
        args = parse_qtop_cmdline_args()
        assert args.sect_2_off is True

    def test_disable_section3(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-3"])
        args = parse_qtop_cmdline_args()
        assert args.sect_3_off is True

    def test_account_totals(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "--accounttotals"])
        args = parse_qtop_cmdline_args()
        assert args.SHOW_ACCOUNT_TOTALS is True

    def test_blind_remapping(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-a"])
        args = parse_qtop_cmdline_args()
        assert args.BLINDREMAP is True

    def test_source_dir(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-s", "/tmp/data"])
        args = parse_qtop_cmdline_args()
        assert args.SOURCEDIR == "/tmp/data"

    def test_watch_with_default(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-w"])
        args = parse_qtop_cmdline_args()
        assert args.WATCH == 2

    def test_watch_with_value(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-w", "10"])
        args = parse_qtop_cmdline_args()
        assert args.WATCH == 10

    def test_transpose(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-T"])
        args = parse_qtop_cmdline_args()
        assert args.TRANSPOSE is True

    def test_web_enabled(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-B"])
        args = parse_qtop_cmdline_args()
        assert args.WEB is True

    def test_custom_config_file(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-f", "/tmp/myconfig.yaml"])
        args = parse_qtop_cmdline_args()
        assert args.CONFFILE == "/tmp/myconfig.yaml"

    def test_strict_check(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-S"])
        args = parse_qtop_cmdline_args()
        assert args.STRICTCHECK is True

    def test_less_mode(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-l"])
        args = parse_qtop_cmdline_args()
        assert args.LESS is True

    def test_force_names(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-F"])
        args = parse_qtop_cmdline_args()
        assert args.FORCE_NAMES is True

    def test_experimental(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["qtop", "-e"])
        args = parse_qtop_cmdline_args()
        assert args.EXPERIMENTAL is True
