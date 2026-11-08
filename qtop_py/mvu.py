## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Explicit model/update/query/view boundaries; I/O effects belong to the runtime."""

import copy
import re
import signal
from itertools import groupby

from qtop_py import utils
from qtop_py.colormap import color_to_code, queue_to_color
from qtop_py.constants import SYMBOL_LONG_TAIL_USER
from qtop_py.schedulers.metadata import NODE_JOB_FIELDS
from qtop_py.ui.controls import NAVIGATION_KEYS


def MVU_reserved_pattern_mappings(config):
    """Return display symbols that must never be interpreted as user patterns."""
    return {
        config["non_existent_node_symbol"]: "#",
        "_": "_",
        SYMBOL_LONG_TAIL_USER: "account_not_colored",
        config["SEPARATOR"]: "account_not_colored",
    }


def MVU_compress_colored_line(value):
    """Coalesce adjacent characters that use the same ANSI color code."""
    color_char_pairs = [item for item in re.split(r"\x1b\[0;m", value) if item]
    return "".join(color + "".join(item[-1] for item in items) + "\x1b[0;m" for color, items in groupby(color_char_pairs, key=lambda item: item[:-1]))


def MVU_gauge_core_vectors(core_user_map, node_column_start, node_column_stop, coreline_notthere_or_unused, non_existent_symbol, remove_corelines):
    """
    generator that loops over each core user vector and yields a boolean stating whether the core vector can be omitted via
    REM_EMPTY_CORELINES or its respective switch
    """
    delta = node_column_stop - node_column_start
    for ind, k in enumerate(core_user_map.copy()):
        core_x_vector = core_user_map["Core" + str(ind) + "vector"][node_column_start:node_column_stop]
        core_x_str = "".join(str(x) for x in core_x_vector)
        yield core_x_vector, ind, k, coreline_notthere_or_unused(non_existent_symbol, remove_corelines, delta, core_x_str)


def MVU_keep_queue_initials_only_and_colorize(worker_nodes, queue_to_color):
    return [
        dict(
            worker_node,
            qname=[utils.ColorStr(queue, color=queue_to_color.get(queue, "")) for queue in worker_node["qname"]],
        )
        for worker_node in worker_nodes
    ]


def MVU_colorize_nodestate(worker_nodes, nodestate_to_color):
    return [
        dict(
            worker_node,
            state=[utils.ColorStr(nodestate, color=nodestate_to_color.get(worker_node["state"], "")) for nodestate in worker_node["state"]],
        )
        for worker_node in worker_nodes
    ]


def MVU_colorize(model, text, color_func=None, pattern="NoPattern", mapping=None, bg_color=None, bold=False):
    """
    prints colored text according to at least one of the keyword arguments available:
    color_func can be a direct ansi color name or a function providing a color name.
    If color is given directly as color_func, pattern/mapping are not needed.
    If no color_func and no mapping are defined, the default mapping is used,
    which is user_to_color, mapping an account name to a color.
    A pattern must then be given, which is the key of the mapping.
    Other mappings available are: nodestate_to_color, queue_to_color.
    Examples:
    s = ColorStr(string='This is some text', color='Red_L')
    print(colorize(s, color_func=s.color), end=" ") # Red_L is applied directly
    print(colorize(s.str, pattern='alicesgm')) # mapping defaults to user_to_color
    print(colorize(s.str, color_func=s.color, bg_color='BlueBG')) # bg and fg colors applied directly

    state = ColorStr('running. coloring according to node state')
    print(colorize(state.str, mapping=nodestate_to_color, pattern=state.initial))
    """
    bg_color = bg_color if bg_color else "NOBG"
    if not mapping:
        mapping = model.user_to_color
    try:
        ansi_color = color_to_code[color_func] if color_func else color_to_code[mapping[pattern]]
    except KeyError:
        return text
    else:
        if bold and ansi_color[0] in "01":
            ansi_color = "1" + ansi_color[1:]
        if model.args.COLOR == "ON" and pattern != "account_not_colored":
            text = "\033[%(fg_color)s%(bg_color)sm%(text)s\033[0;m" % {"fg_color": ansi_color, "bg_color": color_to_code[bg_color], "text": text}

        return text


class MVU_Model(object):
    def __init__(self, config, args, viewport, backend=None, display_overrides=None, document=None, user_to_color=None, nodestate_to_color=None, package_path="", help_file=""):
        self.config, self.args, self.viewport = config, args, viewport
        self.backend, self.scheduler = backend, backend.get_mnemonic() if backend is not None else ""
        self.display_overrides = {} if display_overrides is None else display_overrides
        self.document, self.user_to_color = document, {} if user_to_color is None else user_to_color
        self.nodestate_to_color = {} if nodestate_to_color is None else nodestate_to_color
        self.queue_to_color = queue_to_color
        self.package_path, self.help_file = package_path, help_file
        self.transposed_sections, self.user_details = [], {}

    def MVU_colorize(self, *args, **kwargs):
        return MVU_colorize(self, *args, **kwargs)

    def MVU_node_job_ids(self, node):
        return MVU_node_job_ids(node, self.backend)

    def MVU_reset_sigpipe(self):
        value = getattr(signal, "SIGPIPE", None)
        if value is not None:
            signal.signal(value, signal.SIG_DFL)


def MVU_node_job_ids(node, backend=None):
    jobs = set(node["core_job_map"].values())
    fields = NODE_JOB_FIELDS if backend is None else set(NODE_JOB_FIELDS).union(backend.node_job_fields)
    for field in fields:
        jobs.update(node.get(field, []))
    return jobs


def MVU_update(model, message, payload=None):
    """Return the next model and effect requests without performing terminal or scheduler I/O."""
    keys = set(NAVIGATION_KEYS) | {"snapshot", "layout", "t", "Q", "m", "q", "F", "s", "f", "H", "?", "r"}
    if message not in keys:
        return model, ()
    next_model = copy.copy(model)
    next_model.display_overrides = model.display_overrides.copy()
    next_model.viewport = copy.copy(model.viewport)
    effects = []
    if message == "snapshot":
        if payload is None:
            raise ValueError("Snapshot update requires collected data")
        next_model.document, next_model.config, next_model.args, next_model.backend, next_model.user_to_color, next_model.nodestate_to_color = payload
        next_model.scheduler = next_model.backend.get_mnemonic()
    elif message == "layout":
        next_model.viewport = copy.copy(payload)
    elif message in NAVIGATION_KEYS:
        method, text, bounded = NAVIGATION_KEYS[message]
        moved = getattr(next_model.viewport, method)()
        effects.append(("notice", text if not bounded or moved else "Staying put"))
    elif message == "t":
        next_model.display_overrides["transpose_wn_matrices"] = not next_model.display_overrides.get("transpose_wn_matrices", next_model.config["transpose_wn_matrices"])
        next_model.viewport.reset_display()
        effects.append(("notice", "Transposing matrix..."))
    elif message == "Q":
        next_model.display_overrides["queue_summary_expanded"] = not next_model.display_overrides.get("queue_summary_expanded", getattr(next_model.args, "EXPAND_QUEUES", False))
    elif message == "m":
        mapping = "queue_to_color" if next_model.display_overrides.get("core_coloring", "user_to_color") == "user_to_color" else "user_to_color"
        next_model.display_overrides["core_coloring"] = mapping
        effects.append(("notice", "Changing to " + ("queue" if mapping == "queue_to_color" else "user")))
    elif message == "F":
        next_model.display_overrides["force_names"] = not next_model.display_overrides.get("force_names", False)
        effects.append(("notice", "Toggling full-name/incremental nr WN labels"))
    elif message == "?":
        next_model.viewport.reset_display()
        if "output_fp" in next_model.display_overrides:
            next_model.display_overrides.pop("output_fp")
        else:
            next_model.display_overrides["output_fp"] = next_model.help_file
        effects.append(("notice", "opening help..."))
    elif message == "r":
        mode = (next_model.display_overrides.get("rem_empty_corelines", next_model.config["rem_empty_corelines"]) + 1) % 3
        next_model.display_overrides["rem_empty_corelines"] = mode
        effects.append(("notice", ("Showing all corelines", 'Hiding not-really-there ("#") corelines', 'Hiding all unused ("#" and "_") corelines')[mode]))
    else:
        effects.append(({"q": "quit", "s": "sort", "f": "filter", "H": "highlight"}[message], None))
    return next_model, tuple(effects)
