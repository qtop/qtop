## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Explicit model/update/query/view boundaries; I/O effects belong to the runtime."""

import copy
import re
import signal
from itertools import groupby
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple

from qtop_py import utils
from qtop_py.colormap import color_to_code, queue_to_color
from qtop_py.constants import SYMBOL_LONG_TAIL_USER
from qtop_py.schedulers.metadata import NODE_JOB_FIELDS
from qtop_py.ui.controls import NAVIGATION_KEYS


def MVU_reserved_pattern_mappings(config: Mapping[str, Any]) -> Dict[str, str]:
    """Return display symbols that must never be interpreted as user patterns."""
    return {
        config["non_existent_node_symbol"]: "#",
        "_": "_",
        SYMBOL_LONG_TAIL_USER: "account_not_colored",
        config["SEPARATOR"]: "account_not_colored",
    }


def MVU_compress_colored_line(value: str) -> str:
    """Coalesce adjacent characters that use the same ANSI color code."""
    if "\x1b[" not in value:  # uncoloured (-c OFF): nothing to coalesce, add no reset
        return value
    color_char_pairs = [item for item in re.split(r"\x1b\[0;m", value) if item]
    return "".join(color + "".join(item[-1] for item in items) + "\x1b[0;m" for color, items in groupby(color_char_pairs, key=lambda item: item[:-1]))


def MVU_gauge_core_vectors(
    core_user_map: Dict[str, Any], node_column_start: int, node_column_stop: int, coreline_notthere_or_unused: Callable[..., bool], non_existent_symbol: str, remove_corelines: int
) -> Iterator[Tuple[Any, int, str, bool]]:
    """Yield each core line with a flag telling whether REM_EMPTY_CORELINES may omit it."""
    delta = node_column_stop - node_column_start
    for ind, k in enumerate(core_user_map.copy()):
        core_x_vector = core_user_map["Core" + str(ind) + "vector"][node_column_start:node_column_stop]
        core_x_str = "".join(str(x) for x in core_x_vector)
        yield core_x_vector, ind, k, coreline_notthere_or_unused(non_existent_symbol, remove_corelines, delta, core_x_str)


def MVU_keep_queue_initials_only_and_colorize(worker_nodes: Iterable[Mapping[str, Any]], queue_to_color: Mapping[str, str]) -> List[Dict[str, Any]]:
    """Return copies of the worker nodes with each queue name as a coloured ColorStr."""
    return [
        dict(
            worker_node,
            qname=[utils.ColorStr(queue, color=queue_to_color.get(queue, "")) for queue in worker_node["qname"]],
        )
        for worker_node in worker_nodes
    ]


def MVU_colorize_nodestate(worker_nodes: Iterable[Mapping[str, Any]], nodestate_to_color: Mapping[str, str]) -> List[Dict[str, Any]]:
    """Return copies of the worker nodes with their state coloured by nodestate_to_color."""
    return [
        dict(
            worker_node,
            state=[utils.ColorStr(nodestate, color=nodestate_to_color.get(worker_node["state"], "")) for nodestate in worker_node["state"]],
        )
        for worker_node in worker_nodes
    ]


def MVU_palette_order(model: "MVU_Model") -> List[str]:
    """Return the palettes that key p cycles through, from palettes.yaml."""
    return list((model.config or {}).get("palettes", {}).get("PALETTES") or ["default"])


def MVU_palette(model: "MVU_Model") -> Dict[str, str]:
    """Return the current palette's colour codes; empty for the default colours."""
    name = model.display_overrides.get("palette", MVU_palette_order(model)[0])
    return (model.config or {}).get("palettes", {}).get(name) or {}


def MVU_colorize(
    model: "MVU_Model",
    text: Any,
    color_func: Optional[str] = None,
    pattern: str = "NoPattern",
    mapping: Optional[Mapping[str, str]] = None,
    bg_color: Optional[str] = None,
    bold: bool = False,
) -> Any:
    """Return text wrapped in the ANSI colour given by color_func, or found by pattern in mapping.

    color_func is a colour name or a function returning one; then pattern and mapping are not needed.
    Otherwise pattern is looked up in mapping, which defaults to user_to_color (account name to colour);
    nodestate_to_color and queue_to_color are the other mappings.

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
    palette = MVU_palette(model)
    try:
        name = color_func if color_func else mapping[pattern]
        ansi_color = palette.get(name, color_to_code[name])
    except KeyError:
        return text
    else:
        if bold and ansi_color[0] in "01":
            ansi_color = "1" + ansi_color[1:]
        if model.args.COLOR == "ON" and pattern != "account_not_colored":
            text = "\033[%(fg_color)s%(bg_color)sm%(text)s\033[0;m" % {"fg_color": ansi_color, "bg_color": palette.get(bg_color, color_to_code[bg_color]), "text": text}

        return text


class MVU_Model(object):
    """Everything one qtop frame needs: configuration, options, viewport, scheduler backend, snapshot and colour maps."""

    def __init__(
        self,
        config: Dict[str, Any],
        args: Any,
        viewport: Any,
        backend: Any = None,
        display_overrides: Optional[Dict[str, Any]] = None,
        snapshot: Any = None,
        user_to_color: Optional[Dict[str, str]] = None,
        nodestate_to_color: Optional[Dict[str, str]] = None,
        package_path: str = "",
        help_file: str = "",
    ) -> None:
        self.config, self.args, self.viewport = config, args, viewport
        self.backend, self.scheduler = backend, backend.get_mnemonic() if backend is not None else ""
        self.display_overrides = {} if display_overrides is None else display_overrides
        self.snapshot, self.user_to_color = snapshot, {} if user_to_color is None else user_to_color
        self.nodestate_to_color = {} if nodestate_to_color is None else nodestate_to_color
        self.queue_to_color = queue_to_color
        self.package_path, self.help_file = package_path, help_file
        self.transposed_sections: List[str] = []
        self.user_details: Dict[str, Any] = {}

    def MVU_colorize(self, *args: Any, **kwargs: Any) -> Any:
        """Colour text with this model's colour maps; see MVU_colorize."""
        return MVU_colorize(self, *args, **kwargs)

    def MVU_node_job_ids(self, node: Mapping[str, Any]) -> Set[str]:
        """Return the job IDs on a node, including the backend's extra node job fields."""
        return MVU_node_job_ids(node, self.backend)

    def MVU_reset_sigpipe(self) -> None:
        """Restore the default SIGPIPE action, so piping into head exits quietly."""
        value = getattr(signal, "SIGPIPE", None)
        if value is not None:
            signal.signal(value, signal.SIG_DFL)


def MVU_node_job_ids(node: Mapping[str, Any], backend: Any = None) -> Set[str]:
    """Return the job IDs on a node: its core map plus the standard and backend-specific job fields."""
    jobs = set(node["core_job_map"].values())
    fields = NODE_JOB_FIELDS if backend is None else set(NODE_JOB_FIELDS).union(backend.node_job_fields)
    for field in fields:
        jobs.update(node.get(field, []))
    return jobs


def MVU_update(model: "MVU_Model", message: str, payload: Any = None) -> Tuple["MVU_Model", Tuple[Tuple[str, Optional[str]], ...]]:
    """Return the next model and effect requests without performing terminal or scheduler I/O."""
    keys = set(NAVIGATION_KEYS) | {"snapshot", "layout", "t", "Q", "m", "q", "F", "s", "f", "H", "?", "r", "p", "1", "2", "3", "4"}
    if message not in keys:
        return model, ()
    next_model = copy.copy(model)
    next_model.display_overrides = model.display_overrides.copy()
    next_model.viewport = copy.copy(model.viewport)
    effects: List[Tuple[str, Optional[str]]] = []
    if message == "snapshot":
        if payload is None:
            raise ValueError("Snapshot update requires collected data")
        next_model.snapshot, next_model.config, next_model.args, next_model.backend, next_model.user_to_color, next_model.nodestate_to_color = payload
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
    elif message in ("1", "2", "3", "4"):  # as -1, -2, -3 hide sections 1-3 and -4 the accounts totals row
        key = "SHOW_ACCOUNT_TOTALS" if message == "4" else "sect_%s_off" % message
        next_model.display_overrides[key] = not next_model.display_overrides.get(key, getattr(next_model.args, key, message == "4"))
        effects.append(("notice", "Toggling the accounts totals row" if message == "4" else "Toggling section " + message))
    elif message == "p":
        order = MVU_palette_order(next_model)
        current = next_model.display_overrides.get("palette", order[0])
        next_model.display_overrides["palette"] = order[(order.index(current) + 1) % len(order)] if current in order else order[0]
        effects.append(("notice", "Palette: " + next_model.display_overrides["palette"]))
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
