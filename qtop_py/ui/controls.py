## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Small keyboard operations independent of runtime globals."""

import codecs
import os
import select
from typing import Any, Dict, List, Mapping

from qtop_py.constants import KEY_SEQUENCE_TIMEOUT

NAVIGATION_KEYS = {
    "j": ("scroll_down", "Going down...", True),
    " ": ("scroll_down", "Going down...", True),
    "k": ("scroll_up", "Going up...", True),
    "\x7f": ("scroll_up", "Going up...", True),
    "l": ("scroll_right", "Going right...", False),
    "h": ("scroll_left", "Going left...", False),
    "$": ("scroll_far_right", "Going far right...", False),
    "0": ("scroll_far_left", "Going far left...", False),
    "J": ("scroll_bottom", "Going to the bottom...", True),
    "G": ("scroll_bottom", "Going to the bottom...", True),
    "K": ("scroll_top", "Going to the top...", False),
    "g": ("scroll_top", "Going to the top...", False),
    "R": ("reset_display", "Resetting display...", False),
    "up": ("scroll_line_up", "Going up...", True),
    "down": ("scroll_line_down", "Going down...", True),
    "left": ("scroll_left", "Going left...", False),
    "right": ("scroll_right", "Going right...", False),
    "home": ("scroll_top", "Going to the top...", False),
    "end": ("scroll_bottom", "Going to the bottom...", True),
    "pageup": ("scroll_up", "Going up...", False),
    "pagedown": ("scroll_down", "Going down...", True),
}


class KeyboardInput:
    """Read complete terminal keys without hiding queued bytes in TextIO buffers."""

    def __init__(self, stream: Any) -> None:
        self.stream, self.pending = stream, ""

    def _character(self) -> str:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while True:
            raw = os.read(self.stream.fileno(), 1)
            if not raw:
                raise EOFError
            char = decoder.decode(raw)
            if char:
                return char

    def read(self) -> str:
        """Read one character or a bounded CSI/SS3 navigation sequence."""
        if self.pending:
            char, self.pending = self.pending, ""
        else:
            char = self._character()
        if char != "\x1b" or not select.select([self.stream], [], [], KEY_SEQUENCE_TIMEOUT)[0]:
            return char
        prefix = self._character()
        if prefix not in ("[", "O"):
            self.pending = prefix
            return char
        sequence = ""
        while len(sequence) < 16 and select.select([self.stream], [], [], KEY_SEQUENCE_TIMEOUT)[0]:
            sequence += self._character()
            if sequence[-1].isalpha() or sequence[-1] == "~":
                break
        return {
            "A": "up",
            "B": "down",
            "C": "right",
            "D": "left",
            "H": "home",
            "F": "end",
            "1~": "home",
            "7~": "home",
            "4~": "end",
            "8~": "end",
            "5~": "pageup",
            "6~": "pagedown",
        }.get(sequence, "")


def read_rule_choices(prompt: str, choices: Mapping[int, str], rules: List[Dict[str, List[str]]]) -> None:
    """Read menu choices and their arguments until Enter, appending each as a rule."""
    while True:
        answer = input("\nChoose %s command, or Enter to exit:-> " % prompt)
        if not answer:
            return
        try:
            choice = int(answer)
        except ValueError:
            return
        if choice not in choices:
            return
        arguments = []
        while True:
            answer = input("\nEnter argument, or Enter to exit:-> ")
            if not answer:
                break
            arguments.append(answer)
        rules.append({choices[choice]: arguments})
