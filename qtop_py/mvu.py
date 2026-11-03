## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Explicit model/update/query/view boundaries; I/O effects belong to the runtime."""

import copy
import re
import signal
from collections import namedtuple
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


def MVU_gauge_core_vectors(core_user_map, print_char_start, print_char_stop, coreline_notthere_or_unused, non_existent_symbol, remove_corelines):
    """
    generator that loops over each core user vector and yields a boolean stating whether the core vector can be omitted via
    REM_EMPTY_CORELINES or its respective switch
    """
    delta = print_char_stop - print_char_start
    for ind, k in enumerate(core_user_map.copy()):
        core_x_vector = core_user_map["Core" + str(ind) + "vector"][print_char_start:print_char_stop]
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


def MVU_colorize_nodestate(worker_nodes, nodestate_to_color, ffunc):
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
    bg_color = "NOBG" if not bg_color else bg_color
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
    def __init__(self, config, args, viewport, backend=None, settings=None, document=None, user_to_color=None, nodestate_to_color=None, package_path="", help_file=""):
        self.config, self.args, self.viewport = config, args, viewport
        self.backend, self.scheduler = backend, backend.get_mnemonic() if backend is not None else ""
        self.settings = {} if settings is None else settings
        self.document, self.user_to_color = document, {} if user_to_color is None else user_to_color
        self.nodestate_to_color = {} if nodestate_to_color is None else nodestate_to_color
        self.queue_to_color = queue_to_color
        self.package_path, self.help_file = package_path, help_file
        self.transposed_matrices, self.details = [], {}

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
    if message == "refresh":
        return model, (("collect", None),)
    keys = set(NAVIGATION_KEYS) | {"snapshot", "layout", "t", "Q", "m", "q", "F", "s", "f", "H", "?", "r"}
    if message not in keys:
        return model, ()
    updated = copy.copy(model)
    updated.settings = model.settings.copy()
    updated.viewport = copy.copy(model.viewport)
    effects = []
    if message == "snapshot":
        if payload is None:
            raise ValueError("Snapshot update requires collected data")
        updated.document, updated.config, updated.args, updated.backend, updated.user_to_color, updated.nodestate_to_color = payload
        updated.scheduler = updated.backend.get_mnemonic()
    elif message == "layout":
        updated.viewport = copy.copy(payload)
    elif message in NAVIGATION_KEYS:
        method, text, bounded = NAVIGATION_KEYS[message]
        moved = getattr(updated.viewport, method)()
        effects.append(("notice", text if not bounded or moved else "Staying put"))
    elif message == "t":
        updated.settings["transpose_wn_matrices"] = not updated.settings.get("transpose_wn_matrices", updated.config["transpose_wn_matrices"])
        updated.viewport.reset_display()
        effects.append(("notice", "Transposing matrix..."))
    elif message == "Q":
        updated.settings["queue_summary_expanded"] = not updated.settings.get("queue_summary_expanded", getattr(updated.args, "EXPAND_QUEUES", False))
    elif message == "m":
        mapping = "queue_to_color" if updated.settings.get("core_coloring", "user_to_color") == "user_to_color" else "user_to_color"
        updated.settings["core_coloring"] = mapping
        effects.append(("notice", "Changing to " + ("queue" if mapping == "queue_to_color" else "user")))
    elif message == "F":
        updated.settings["force_names"] = not updated.settings.get("force_names", False)
        effects.append(("notice", "Toggling full-name/incremental nr WN labels"))
    elif message == "?":
        updated.viewport.reset_display()
        if "output_fp" in updated.settings:
            updated.settings.pop("output_fp")
        else:
            updated.settings["output_fp"] = updated.help_file
        effects.append(("notice", "opening help..."))
    elif message == "r":
        mode = (updated.settings.get("rem_empty_corelines", updated.config["rem_empty_corelines"]) + 1) % 3
        updated.settings["rem_empty_corelines"] = mode
        effects.append(("notice", ("Showing all corelines", 'Hiding not-really-there ("#") corelines', 'Hiding all unused ("#" and "_") corelines')[mode]))
    else:
        effects.append(({"q": "quit", "s": "sort", "f": "filter", "H": "highlight"}[message], None))
    return updated, tuple(effects)


MVU_Projection = namedtuple("MVU_Projection", "model cluster occupancy")


def MVU_query(model):
    """Build private derived nodes/layout; never mutate the canonical snapshot or input model."""
    from qtop_py.mvu_query import Cluster, WNFilter, WNOccupancy

    context = copy.copy(model)
    context.args = copy.copy(model.args)
    context.viewport = copy.copy(model.viewport)
    context.transposed_matrices = []
    nodes = MVU_keep_queue_initials_only_and_colorize(copy.deepcopy(model.document.worker_nodes), model.queue_to_color)
    nodes = MVU_colorize_nodestate(nodes, model.nodestate_to_color, context.MVU_colorize)
    cluster = Cluster(model.document, nodes, WNFilter, model.config, context.args, MVU_model=context)
    occupancy = WNOccupancy(cluster, model.config, model.document, model.user_to_color, list(model.document.jobs_dict), MVU_model=context)
    return MVU_Projection(context, cluster, occupancy)


def MVU_view(projection, savepath, sample_filename, logfile):
    from qtop_py.ui.mvu_view import TextDisplay

    model, cluster, occupancy = projection
    display = TextDisplay(model.document, model.config, model.viewport, occupancy, cluster, model.args, MVU_model=model)
    display.display_selected_sections(savepath, sample_filename, logfile)
    return display
