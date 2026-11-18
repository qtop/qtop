## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Watch prompts, notices and menus for explicit MVU effects."""

import re
import sys
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from qtop_py.safety import visible_text
from qtop_py.ui.controls import KeyboardInput, read_rule_choices
from qtop_py.ui.terminal import menu_terminal
from qtop_py.ui.viewport import Viewport, terminal_rows


def MVU_effects(
    model: Any,
    effects: Iterable[Tuple[str, Optional[str]]],
    cluster: Any,
    old_attrs: Any,
    new_attrs: Any,
    keyboard: Optional[KeyboardInput] = None,
    output_fp: Optional[str] = None,
) -> None:
    """Execute requested UI I/O outside MVU_update and the snapshot query."""
    for action, value in effects:
        if action == "notice":
            write_notice("%s %s" % (model.MVU_colorize("***", "Green_L"), visible_text(value or "")), model.viewport)
        elif action in ("search", "line"):
            navigate_prompt(model, keyboard or KeyboardInput(sys.stdin), action, output_fp)
        elif action == "quit":
            print(model.MVU_colorize("\nExiting. Thank you for ..watching ;)\n", "Cyan_L"))
            raise SystemExit(0)
        elif action == "sort":
            choose_sort(model.viewport, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)
        elif action == "filter":
            choose_filter(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)
        elif action == "highlight":
            choose_highlight(model.viewport, cluster, model.display_overrides, old_attrs, new_attrs, model.MVU_colorize)


def write_notice(message: str, viewport: Viewport) -> None:
    """Replace the reserved notice row without wrapping or scrolling the frame."""
    row = next(terminal_rows([message], viewport.h_term_size)).rstrip("\n")
    print("\r\x1b[2K" + row, end="", flush=True)


def navigate_prompt(model: Any, keyboard: KeyboardInput, action: str, output_fp: Optional[str]) -> None:
    """Read a literal search or one-based displayed-row address on the notice row."""
    text = ""
    prefix = "/" if action == "search" else ":"
    while True:
        shown = list(terminal_rows([prefix + visible_text(text)], model.viewport.h_term_size))[-1].rstrip("\n")
        print("\r\x1b[2K" + shown, end="", flush=True)
        char = keyboard.read()
        if char in ("\r", "\n", "\x1b"):
            break
        if char in ("\x7f", "\b"):
            text = text[:-1]
        elif char == "\x15":
            text = ""
        elif len(text) < 256 and len(char) == 1 and char.isprintable():
            text += char
    print("\r\x1b[2K", end="", flush=True)
    if char == "\x1b":
        return
    if action == "line":
        if text.isdecimal() and int(text) > 0:
            model.viewport.v_start = int(text) - 1
        else:
            write_notice("Enter a positive displayed line number", model.viewport)
        return
    text = text or model.display_overrides.get("search_text", "")
    if not text or not output_fp:
        return
    model.display_overrides["search_text"] = text
    first = None
    with open(output_fp, encoding="utf-8") as source:
        start = 0
        for line in source:
            visible = re.sub(r"\x1b(?:\[[0-9;:]*m|c)", "", line)
            position = visible.find(text)
            if position >= 0:
                index = start + sum(1 for _ in terminal_rows([visible[: position + 1]], model.viewport.h_term_size)) - 1
                if first is None:
                    first = index
                if index > model.viewport.v_start:
                    model.viewport.v_start = index
                    return
            start += sum(1 for _ in terminal_rows([line], model.viewport.h_term_size))
    if first is not None:
        model.viewport.v_start = first
    else:
        write_notice("Pattern not found", model.viewport)


def choose_sort(viewport: Viewport, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any]) -> None:
    """Ask for a worker-node sort order and store it in the display overrides."""
    sort_map: Dict[str, Any] = OrderedDict()

    sort_map["0"] = ("sort reset", [])
    sort_map["1"] = ("sort by nodename-notnum", [])
    sort_map["2"] = ("sort by nodename-notnum length", [])
    sort_map["3"] = ("sort by all numbers", [])
    sort_map["4"] = ("sort by first letter", [])
    sort_map["5"] = ("sort by node state", [])
    sort_map["6"] = ("sort by nr of cores", [])
    sort_map["7"] = ("sort by core occupancy", [])

    print(
        "Type in sort order. This can be a single number or a sequence of numbers,\n"
        "e.g. to sort first by first word, then by all numbers then by first name length, type 132, then <enter>."
    )

    for nr, sort_method in sort_map.items():
        print("(%s): %s" % (colorize(nr, color_func="Red_L"), sort_method[0]))

    with menu_terminal(old_attrs, new_attrs):
        display_overrides["user_sort"] = []
        while True:
            sort_choice = input(
                "\nChoose sorting order, or Enter to exit:-> ",
            )
            if not sort_choice:
                break

            sort_order = list(sort_choice)
            if not set(sort_order).issubset(sort_map):
                continue

            sort_args = [sort_map[i] for i in sort_order]
            display_overrides["user_sort"] = sort_args
            break
    viewport.reset_display()


def choose_filter(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any]) -> None:
    """Ask for worker-node filters and store them in the display overrides."""
    cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
    filter_map = {
        1: "exclude_node_states",
        2: "exclude_numbers",
        3: "exclude_name_patterns",
        4: "or_include_node_states",
        5: "or_include_numbers",
        6: "or_include_name_patterns",
        7: "include_node_states",
        8: "include_numbers",
        9: "include_name_patterns",
        10: "include_queues",
    }
    print(
        "Filter out nodes by:\n%(one)s state %(two)s number"
        " %(three)s name substring or RegEx pattern"
        % {
            "one": colorize("(1)", color_func="Red_L"),
            "two": colorize("(2)", color_func="Red_L"),
            "three": colorize("(3)", color_func="Red_L"),
        }
    )
    print(
        "Filter in nodes by:\n(any) %(four)s state %(five)s number %(six)s name substring or RegEx pattern\n"
        "(all) %(seven)s state %(eight)s number %(nine)s name substring or RegEx pattern %(ten)s queue"
        % {
            "four": colorize("(4)", color_func="Red_L"),
            "five": colorize("(5)", color_func="Red_L"),
            "six": colorize("(6)", color_func="Red_L"),
            "seven": colorize("(7)", color_func="Red_L"),
            "eight": colorize("(8)", color_func="Red_L"),
            "nine": colorize("(9)", color_func="Red_L"),
            "ten": colorize("(10)", color_func="Red_L"),
        }
    )
    with menu_terminal(old_attrs, new_attrs):
        display_overrides["filtering"] = []
        read_rule_choices("Filter", filter_map, display_overrides["filtering"])
    viewport.reset_display()


def choose_highlight(viewport: Viewport, cluster: Any, display_overrides: Dict[str, Any], old_attrs: Any, new_attrs: Any, colorize: Callable[..., Any]) -> None:
    """Ask for users or queues to highlight and store them in the display overrides."""
    cluster.wn_filter = cluster.WNFilter(cluster.worker_nodes)
    filter_map = {
        1: "or_include_user_id",
        2: "or_include_user_pat",
        3: "or_include_queue",
        4: "include_user_id",
        5: "include_user_pat",
        6: "include_queue",
    }
    print(
        "Highlight cores by:\n"
        "(any) %(one)s userID %(two)s user name (regex) %(three)s queue\n"
        "(all) %(four)s userID %(five)s user name (regex) %(six)s queue"
        % {
            "one": colorize("(1)", color_func="Red_L"),
            "two": colorize("(2)", color_func="Red_L"),
            "three": colorize("(3)", color_func="Red_L"),
            "four": colorize("(4)", color_func="Red_L"),
            "five": colorize("(5)", color_func="Red_L"),
            "six": colorize("(6)", color_func="Red_L"),
        }
    )
    with menu_terminal(old_attrs, new_attrs):
        display_overrides["highlight"] = []
        read_rule_choices("Highlight", filter_map, display_overrides["highlight"])
    viewport.reset_display()
