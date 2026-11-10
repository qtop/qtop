## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Small keyboard operations independent of runtime globals."""

from typing import Dict, List, Mapping

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
}


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
