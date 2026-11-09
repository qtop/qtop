## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Load and prepare qtop configuration without runtime-global dependencies."""

import copy
import logging
import os
from math import isfinite
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qtop_py.state_types import Config

from qtop_py import fileutils, yaml_parser
from qtop_py.colormap import nodestate_to_color_default, user_to_color_default
from qtop_py.constants import (
    MAX_USER_ACCOUNTS_HEADER_LEN,
    QTOPCONF_YAML,
    SYMBOL_LONG_TAIL_USER,
    SYMBOL_UNKNOWN_NODE_STATE,
    USER_ID_RANKING_COUNT_KEYS,
    USER_ID_RANKING_NAME_KEYS,
)
from qtop_py.errors import OperationalError
from qtop_py.safety import ResourceLimitError, literal_eval


def literal(value):
    try:
        return literal_eval(value)
    except ResourceLimitError:
        raise
    except (ValueError, SyntaxError, TypeError):
        return value


def _require(condition, path, expected):
    if not condition:
        raise ValueError("%s: expected %s" % (path, expected))


def _integer(value, path, minimum=0):
    value = literal(value)
    _require(type(value) is int and value >= minimum, path, "an integer >= %s" % minimum)


def _mapping(value, path):
    _require(isinstance(value, dict), path, "a mapping")
    return value


def _text(value, path):
    _require(isinstance(value, str) and bool(value.strip()), path, "nonempty text")


def validate_config(config):
    """Check merged defaults/overrides; retain extensible backend-specific keys."""
    _mapping(config, "configuration")
    _validate_scalars(config)
    _validate_runtime(config)
    _validate_colors(config)
    _validate_state_columns(config)
    _validate_display(config)
    _validate_user_id_ranking(config)
    _validate_user_accounts_header(config)
    _validate_matrix(config)
    _validate_commands(config)


def _validate_scalars(config):
    """Required text, flag and counter settings."""
    for key in ("scheduler", "savepath", "vertical_separator", "non_existent_node_symbol"):
        _text(config.get(key), key)
    for key in ("transpose_wn_matrices", "fill_with_user_firstletter", "faster_xml_parsing", "overwrite_sample_file"):
        value = literal(config.get(key))
        _require(type(value) in (bool, int) and value in (False, True), key, "True, False, 0 or 1")
    for key in ("vertical_separator_every_X_columns", "rem_empty_corelines", "exotic_starting_wn_nr"):
        _integer(config.get(key, 0), key)


def _validate_runtime(config):
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


def _validate_display(config):
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


def _validate_colors(config):
    """User, node-state and queue color mappings."""
    for key in ("user_color_mappings", "nodestate_color_mappings", "queue_color_mappings"):
        entries = config.get(key)
        _require(isinstance(entries, list) or not entries, key, "a list of mappings")
        for entry in entries or []:
            _mapping(entry, key)
            _require(all(isinstance(pattern, str) and isinstance(color, str) for pattern, color in entry.items()), key, "text patterns and color names")


def _validate_state_columns(config):
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


def _validate_matrix(config):
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


def _validate_commands(config):
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


def configured_separator(config):
    separator = config.get("SEPARATOR", config.get("vertical_separator", "|"))
    return separator.replace("'", "") if isinstance(separator, str) else separator


def reserved_user_symbols(config):
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


def available_possible_ids(config):
    reserved_symbols = reserved_user_symbols(config)
    return [symbol for symbol in config["possible_ids"] if symbol not in reserved_symbols]


def normalize_config(raw_config):
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


def get_key_val_from_option_string(string):
    key, separator, val = string.partition("=")
    if not separator or not key.strip():
        raise OperationalError("Invalid -o option %r; expected KEY=VALUE." % string)
    return key, val


def update_config_with_cmdline_vars(args, config):
    config["rem_empty_corelines"] = int(config.get("rem_empty_corelines", 0))
    for opt in getattr(args, "OPTION", ()):
        key, val = get_key_val_from_option_string(opt)
        config[key] = literal(val)

    if getattr(args, "TRANSPOSE", False):
        config["transpose_wn_matrices"] = not config["transpose_wn_matrices"]

    if getattr(args, "REM_EMPTY_CORELINES", 0):
        config["rem_empty_corelines"] += args.REM_EMPTY_CORELINES

    return config


def prepare_config(config, args=None):
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


def load_yaml_config(args, package_dir, initial_cwd, system_dir, user_dir):
    """Load defaults < system < user < custom, then prepare CLI overrides."""
    if getattr(args, "load_sample", None):
        return prepare_config(args.sample_snapshot["config"], args)
    config = yaml_parser.parse(os.path.join(os.path.realpath(package_dir), QTOPCONF_YAML))
    logging.info("Default configuration dictionary loaded. Length: %s items", len(config))
    for label, directory in (("Env", system_dir), ("User", user_dir)):
        try:
            layer = yaml_parser.parse(os.path.join(directory, QTOPCONF_YAML))
        except FileNotFoundError:
            logging.info("%s could not be found in %s/", QTOPCONF_YAML, directory)
        else:
            config.update(layer)
            logging.info("%s configuration loaded from %s/. Length: %s items", label, directory, len(layer))
    if args.CONFFILE:
        for directory in (user_dir, initial_cwd):
            try:
                custom = yaml_parser.parse(os.path.join(directory, args.CONFFILE))
            except FileNotFoundError:
                continue
            config.update(custom)
            logging.info("Custom User configuration loaded from %s/. Length: %s items", directory, len(custom))
            break
        else:
            raise OperationalError("Custom configuration file %s was not found in %s/ or %s/." % (args.CONFFILE, user_dir, initial_cwd))
    logging.info("Updated main dictionary. Length: %s items", len(config))
    return prepare_config(config, args)
