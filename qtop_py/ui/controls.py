## SPDX-License-Identifier: MIT
"""Small keyboard operations independent of runtime globals."""

import logging

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


def navigate_viewport(viewport, key, colorize):
    action = NAVIGATION_KEYS.get(key)
    if action is None:
        return False
    method, message, bounded = action
    logging.debug("v_start: %s", viewport.v_start)
    moved = getattr(viewport, method)()
    print("%s %s" % (colorize("***", "Green_L"), message if not bounded or moved else "Staying put"))
    if key == "$":
        logging.info("h_start: %s", viewport.h_start)
        logging.info("max_width: %s", viewport.max_width)
        logging.info('config["term_size"][1] %s', viewport.h_term_size)
        logging.info("h_stop: %s", viewport.h_stop)
    return True


def read_rule_choices(prompt, choices, rules):
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
