## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Load and prepare qtop configuration without runtime-global dependencies."""

import copy
import logging
import os
import re
import time
from math import isfinite
from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple

if TYPE_CHECKING:
    from typing import Final

    from qtop_py.state_types import Config

from qtop_py import fileutils, yaml_parser
from qtop_py.colormap import color_to_code, nodestate_to_color_default, user_to_color_default
from qtop_py.constants import (
    MAX_USER_ACCOUNTS_HEADER_LEN,
    PALETTES_YAML,
    QTOPCONF_YAML,
    SYMBOL_LONG_TAIL_USER,
    SYMBOL_UNKNOWN_NODE_STATE,
    USER_ID_RANKING_COUNT_KEYS,
    USER_ID_RANKING_NAME_KEYS,
)
from qtop_py.errors import OperationalError
from qtop_py.safety import ResourceLimitError, literal_eval

# Parsed YAML layers by path, each with the stat signature it was parsed under.
# Watch mode reloads the configuration every frame; an unchanged file is
# deep-copied from here instead of being re-parsed, and any edit (new mtime,
# ctime, size or inode) or removal invalidates its entry.
_LAYER_CACHE: Dict[str, Tuple[Tuple[int, ...], Dict[str, Any]]] = {}
# A file modified within this many seconds of being read is not cached: an edit
# in the same mtime tick could otherwise keep an identical signature.
RACY_WINDOW_S: "Final" = 2.0


def _stat_signature(path: str) -> Tuple[int, ...]:
    st = os.stat(path)
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def parse_layer(path: str) -> Dict[str, Any]:
    """Parse one YAML layer, reusing the last parse while the file is unchanged.

    Returns a fresh deep copy each call, so callers may mutate their frame's
    configuration without touching the cache or other frames. Missing or
    unreadable files and parse errors propagate from yaml_parser.parse and are
    never cached.
    """
    key = os.path.abspath(path)  # watch mode may chdir between frames
    try:
        signature = _stat_signature(path)
    except OSError:
        _LAYER_CACHE.pop(key, None)
        return yaml_parser.parse(path)  # uncached: the parser raises the same errors as before caching
    cached = _LAYER_CACHE.get(key)
    if cached is not None and cached[0] == signature:
        return copy.deepcopy(cached[1])
    layer = yaml_parser.parse(path)  # stat first: an edit during the parse changes the signature
    if time.time() - signature[3] / 1e9 > RACY_WINDOW_S:
        _LAYER_CACHE[key] = (signature, copy.deepcopy(layer))
    else:
        _LAYER_CACHE.pop(key, None)
    return layer


def clear_layer_cache() -> None:
    """Forget every cached configuration and palette layer."""
    _LAYER_CACHE.clear()


def literal(value: Any) -> Any:
    """Evaluate a Python literal, or return the value unchanged if it is not one."""
    try:
        return literal_eval(value)
    except ResourceLimitError:
        raise
    except (ValueError, SyntaxError, TypeError):
        return value


def _require(condition: object, path: str, expected: str) -> None:
    if not condition:
        raise ValueError("%s: expected %s" % (path, expected))


def _integer(value: Any, path: str, minimum: int = 0) -> None:
    value = literal(value)
    _require(type(value) is int and value >= minimum, path, "an integer >= %s" % minimum)


def _mapping(value: Any, path: str) -> Any:
    _require(isinstance(value, dict), path, "a mapping")
    return value


def _text(value: Any, path: str) -> None:
    _require(isinstance(value, str) and bool(value.strip()), path, "nonempty text")


def validate_config(config: Any) -> None:
    """Check merged defaults/overrides; retain extensible backend-specific keys."""
    # `config: Any` in the validators: the input is untrusted and these checks establish its shape.
    _mapping(config, "configuration")
    _validate_scalars(config)
    _validate_runtime(config)
    _validate_colors(config)
    _validate_palettes(config)
    _validate_state_columns(config)
    _validate_display(config)
    _validate_user_id_ranking(config)
    _validate_user_accounts_header(config)
    _validate_matrix(config)
    _validate_commands(config)


def _validate_scalars(config: Any) -> None:
    """Required text, flag and counter settings."""
    for key in ("scheduler", "savepath", "vertical_separator", "non_existent_node_symbol"):
        _text(config.get(key), key)
    for key in ("transpose_wn_matrices", "fill_with_user_firstletter", "faster_xml_parsing", "overwrite_sample_file"):
        value = literal(config.get(key))
        _require(type(value) in (bool, int) and value in (False, True), key, "True, False, 0 or 1")
    for key in ("vertical_separator_every_X_columns", "rem_empty_corelines", "exotic_starting_wn_nr"):
        _integer(config.get(key, 0), key)


def _validate_runtime(config: Any) -> None:
    """Command timeout, experimental web endpoint, node id symbols and sorting."""
    timeout = config.get("scheduler_command_timeout", 30)
    try:
        _require(not isinstance(timeout, bool) and isfinite(float(timeout)) and float(timeout) > 0, "scheduler_command_timeout", "positive finite seconds")
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("scheduler_command_timeout: expected positive finite seconds") from error
    host = config.get("experimental_web_host", "127.0.0.1")
    _require(
        isinstance(host, str) and bool(host) and ":" not in host and not any(char.isspace() or ord(char) < 32 for char in host),
        "experimental_web_host",
        "a nonempty IPv4 address or hostname",
    )
    port = literal(config.get("experimental_web_port", 8080))
    _require(type(port) is int and 1 <= port <= 65535, "experimental_web_port", "an integer from 1 to 65535")
    ids = config.get("possible_ids")
    _require(isinstance(ids, (str, list, tuple)) and bool(ids), "possible_ids", "a nonempty sequence of single characters")
    _require(all(isinstance(symbol, str) and len(symbol) == 1 for symbol in ids), "possible_ids", "single characters")
    sorting = _mapping(config.get("sorting"), "sorting")
    reverse = literal(sorting.get("reverse", False))
    _require(type(reverse) in (bool, int) and reverse in (False, True), "sorting.reverse", "True, False, 0 or 1")
    order = sorting.get("user_sort")
    _require(order is None or isinstance(order, list) and all(isinstance(item, str) for item in order), "sorting.user_sort", "a list of sort names or null")


def _validate_user_id_ranking(config: "Config") -> None:
    """Distinct count keys in any order, then exactly one name key last, so ID assignment is a total order."""
    USER_ID_RANKING = config.get("user_id_ranking")
    if USER_ID_RANKING is None:
        return
    expected = "a list of distinct %s keys ending in %s" % ("/".join(USER_ID_RANKING_COUNT_KEYS), " or ".join(USER_ID_RANKING_NAME_KEYS))
    _require(isinstance(USER_ID_RANKING, list) and bool(USER_ID_RANKING), "user_id_ranking", expected)
    COUNT_KEYS, NAME_KEY = USER_ID_RANKING[:-1], USER_ID_RANKING[-1]
    _require(NAME_KEY in USER_ID_RANKING_NAME_KEYS, "user_id_ranking", expected)
    _require(all(key in USER_ID_RANKING_COUNT_KEYS for key in COUNT_KEYS), "user_id_ranking", expected)
    _require(len(set(COUNT_KEYS)) == len(COUNT_KEYS), "user_id_ranking", expected)


def _validate_user_accounts_header(config: "Config") -> None:
    """Single-line note, possibly empty; control characters are escaped at render time."""
    USER_ACCOUNTS_HEADER = config.get("user_accounts_header")
    if USER_ACCOUNTS_HEADER is None:
        return
    _require(
        isinstance(USER_ACCOUNTS_HEADER, str) and "\n" not in USER_ACCOUNTS_HEADER and len(USER_ACCOUNTS_HEADER) <= MAX_USER_ACCOUNTS_HEADER_LEN,
        "user_accounts_header",
        "single-line text of at most %d characters" % MAX_USER_ACCOUNTS_HEADER_LEN,
    )


def _validate_display(config: Any) -> None:
    """SGE whole queues and display sections."""
    whole_queues = config.get("sge_whole_queues")
    _require(
        whole_queues is None or isinstance(whole_queues, list) and all(isinstance(item, str) and bool(item.strip()) for item in whole_queues),
        "sge_whole_queues",
        "a list of queue names or glob patterns",
    )
    parts = config.get("user_display_parts")
    allowed = ("job_accounting_summary", "workernodes_matrix", "user_accounts_pool_mappings")
    _require(isinstance(parts, list) and len(parts) <= 3 and all(part in allowed for part in parts), "user_display_parts", "up to three known display sections")


def _validate_colors(config: Any) -> None:
    """User, node-state and queue color mappings."""
    for key in ("user_color_mappings", "nodestate_color_mappings", "queue_color_mappings"):
        entries = config.get(key)
        _require(isinstance(entries, list) or not entries, key, "a list of mappings")
        for entry in entries or []:
            _mapping(entry, key)
            _require(all(isinstance(pattern, str) and isinstance(color, str) for pattern, color in entry.items()), key, "text patterns and color names")
    colours = config.get("summary_colours")
    _require(not colours or (isinstance(colours, list) and all(colour in color_to_code for colour in colours)), "summary_colours", "a list of colormap color names")


def _validate_palettes(config: Any) -> None:
    """Palettes from palettes.yaml: PALETTES names defined palettes, each mapping colour names to SGR codes."""
    palettes = _mapping(config.get("palettes") or {}, "palettes")
    order = palettes.get("PALETTES", ["default"])
    _require(isinstance(order, list) and order and all(name == "default" or name in palettes for name in order), "palettes.PALETTES", "a list of defined palette names")
    for name, codes in palettes.items():
        valid = isinstance(codes, dict) and all(colour in color_to_code and re.fullmatch(r"[0-9]+(;[0-9]+)*", str(code)) for colour, code in codes.items())
        _require(name == "PALETTES" or valid, "palettes." + name, "colormap colour names mapped to SGR codes (digits and ;)")


def load_palettes(directories: Tuple[str, ...]) -> Dict[str, Any]:
    """Merge palettes.yaml from each directory in turn, palette by palette and colour by colour."""
    palettes: Dict[str, Any] = {}
    for directory in directories:
        try:
            layer = parse_layer(os.path.join(directory, PALETTES_YAML))
        except FileNotFoundError:
            continue
        for name, entries in layer.items():
            palettes[name] = entries if name == "PALETTES" or not isinstance(entries, dict) else dict(palettes.get(name, {}), **entries)
    return palettes


def _validate_state_columns(config: Any) -> None:
    """accounts_states and summary_states must name known state keys."""
    known_states = {"other_of_user", "unknown_of_user"}
    for states in (config.get("state_abbreviations") or {}).values():
        known_states.update(str(key) for key in (states or {}).values())
    for key in ("accounts_states", "summary_states"):
        entries = config.get(key)
        _require(entries is None or isinstance(entries, list) and bool(entries), key, "a nonempty list of state keys")
        for entry in entries or ():
            if isinstance(entry, dict):
                _require(len(entry) == 1 and all(isinstance(label, str) and bool(label.strip()) for label in entry.values()), key, "state keys or {state key: label} mappings")
                entry = next(iter(entry))
            _require(entry in known_states, key, "state keys from state_abbreviations, other_of_user or unknown_of_user")


def _validate_matrix(config: Any) -> None:
    """Worker-node matrix definitions and terminal size."""
    matrices = config.get("workernodes_matrix")
    _require(isinstance(matrices, list) and bool(matrices), "workernodes_matrix", "a nonempty list of mappings")
    for index, entry in enumerate(matrices):
        _mapping(entry, "workernodes_matrix[%s]" % index)
        _require(len(entry) == 1, "workernodes_matrix[%s]" % index, "one matrix definition")
        for name, settings in entry.items():
            _mapping(settings, "workernodes_matrix.%s" % name)
    labels = _mapping(matrices[0].get("wn id lines"), "workernodes_matrix[0].wn id lines")
    for key in ("max_len", "min_masking_threshold", "user_cut_matrix_width"):
        _integer(labels.get(key), "workernodes_matrix.wn id lines.%s" % key, 1 if key == "max_len" else 0)
    _require(
        isinstance(labels.get("alt_label_colors"), list) and bool(labels["alt_label_colors"]) and all(isinstance(color, str) for color in labels["alt_label_colors"]),
        "workernodes_matrix.wn id lines.alt_label_colors",
        "a nonempty list of color names",
    )
    if "term_size" in config:
        size = literal(config["term_size"])
        _require(isinstance(size, (list, tuple)) and len(size) == 2, "term_size", "[rows, columns]")
        _require(all(type(literal(value)) is int and literal(value) > 0 for value in size), "term_size", "two positive integers")


def _validate_commands(config: Any) -> None:
    """Scheduler command pairs, signature commands and extract_info."""
    for key in ("schedulers", "signature_commands", "state_abbreviations"):
        _mapping(config.get(key), key)
    for name, entries in config["schedulers"].items():
        for key, command in _mapping(entries, "schedulers.%s" % name).items():
            path = "schedulers.%s.%s" % (name, key)
            _text(command, path)
            _require(", " in command and all(part.strip() for part in command.split(", ", 1)), path, "a filename, command pair")
    for name, command in config["signature_commands"].items():
        _text(command, "signature_commands.%s" % name)
    info = config.get("extract_info")
    if info:
        _mapping(info, "extract_info")
        _integer(info.get("field_to_use", 5), "extract_info.field_to_use", 2)
        _require(info.get("regex") is None or isinstance(info["regex"], str), "extract_info.regex", "text or null")


def configured_separator(config: Dict[str, Any]) -> Any:
    """Return the matrix column separator, without quotes."""
    separator = config.get("SEPARATOR", config.get("vertical_separator", "|"))
    return separator.replace("'", "") if isinstance(separator, str) else separator


def reserved_user_symbols(config: Dict[str, Any]) -> Set[str]:
    """Return the symbols that may not serve as user IDs."""
    return set(
        symbol
        for symbol in (
            config.get("non_existent_node_symbol", "#"),
            configured_separator(config),
            "_",
            SYMBOL_LONG_TAIL_USER,
            SYMBOL_UNKNOWN_NODE_STATE,
        )
        if symbol
    )


def available_possible_ids(config: Dict[str, Any]) -> List[str]:
    """Return the configured user ID symbols, minus the reserved ones."""
    reserved_symbols = reserved_user_symbols(config)
    return [symbol for symbol in config["possible_ids"] if symbol not in reserved_symbols]


def normalize_config(raw_config: Dict[str, Any]) -> Dict[str, Any]:
    """Return a runtime-ready configuration without mutating parser output."""
    config = copy.deepcopy(raw_config)
    config["possible_ids"] = list(config["possible_ids"])
    for key in (
        "transpose_wn_matrices",
        "fill_with_user_firstletter",
        "faster_xml_parsing",
        "vertical_separator_every_X_columns",
        "overwrite_sample_file",
    ):
        config[key] = literal(config[key])
    config["sorting"]["reverse"] = literal(config["sorting"].get("reverse", "0"))
    wn_id_lines = config["workernodes_matrix"][0]["wn id lines"]
    config["ALT_LABEL_COLORS"] = yaml_parser.fix_config_list(wn_id_lines["alt_label_colors"])
    config["SEPARATOR"] = config["vertical_separator"].replace("'", "")
    config["USER_CUT_MATRIX_WIDTH"] = int(wn_id_lines["user_cut_matrix_width"])
    return config


def get_key_val_from_option_string(string: str) -> Tuple[str, Any]:
    """Split a -o KEY=VALUE option; raise OperationalError if it is malformed."""
    key, separator, val = string.partition("=")
    if not separator or not key.strip():
        raise OperationalError("Invalid -o option %r; expected KEY=VALUE." % string)
    return key, val


def update_config_with_cmdline_vars(args: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply the -o, -T and -E switches to the configuration."""
    config["rem_empty_corelines"] = int(config.get("rem_empty_corelines", 0))
    for opt in getattr(args, "OPTION", ()):
        key, val = get_key_val_from_option_string(opt)
        config[key] = literal(val)

    if getattr(args, "TRANSPOSE", False):
        config["transpose_wn_matrices"] = not config["transpose_wn_matrices"]

    if getattr(args, "REM_EMPTY_CORELINES", 0):
        config["rem_empty_corelines"] += args.REM_EMPTY_CORELINES

    return config


def prepare_config(config: Dict[str, Any], args: Any = None) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, str]]:
    """Validate and normalise the configuration; return it with the user and node-state colour maps."""
    validate_config(config)
    config = normalize_config(config)

    config["user_color_mappings"] = config["user_color_mappings"] or []
    config["nodestate_color_mappings"] = config["nodestate_color_mappings"] or []
    user_to_color = user_to_color_default.copy()
    for rule in config["user_color_mappings"]:
        user_to_color.update(rule)
    nodestate_to_color = nodestate_to_color_default.copy()
    for rule in config["nodestate_color_mappings"]:
        nodestate_to_color.update(rule)
    config["remapping"] = config["remapping"] or []
    config["possible_ids"] = available_possible_ids(config)

    if args is not None:
        config = update_config_with_cmdline_vars(args, config)
        validate_config(config)
    config["savepath"] = fileutils.private_save_directory(config["savepath"])
    logging.debug("%s files will be saved in directory %s.", config["scheduler"], config["savepath"])

    return config, user_to_color, nodestate_to_color


def load_yaml_config(args: Any, package_dir: str, initial_cwd: str, system_dir: str, user_dir: str) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, str]]:
    """Load defaults < system < user < custom, then prepare CLI overrides."""
    if getattr(args, "load_sample", None):
        return prepare_config(args.sample_snapshot["config"], args)
    config = parse_layer(os.path.join(os.path.realpath(package_dir), QTOPCONF_YAML))
    logging.info("Default configuration dictionary loaded. Length: %s items", len(config))
    for label, directory in (("Env", system_dir), ("User", user_dir)):
        try:
            layer = parse_layer(os.path.join(directory, QTOPCONF_YAML))
        except FileNotFoundError:
            logging.info("%s could not be found in %s/", QTOPCONF_YAML, directory)
        else:
            config.update(layer)
            logging.info("%s configuration loaded from %s/. Length: %s items", label, directory, len(layer))
    if args.CONFFILE:
        for directory in (user_dir, initial_cwd):
            try:
                custom = parse_layer(os.path.join(directory, args.CONFFILE))
            except FileNotFoundError:
                continue
            config.update(custom)
            logging.info("Custom User configuration loaded from %s/. Length: %s items", directory, len(custom))
            break
        else:
            raise OperationalError("Custom configuration file %s was not found in %s/ or %s/." % (args.CONFFILE, user_dir, initial_cwd))
    if "palettes" not in config:  # not setdefault(): that would load every palettes.yaml even when a layer already set them
        config["palettes"] = load_palettes((os.path.realpath(package_dir), system_dir, user_dir))
    logging.info("Updated main dictionary. Length: %s items", len(config))
    return prepare_config(config, args)
