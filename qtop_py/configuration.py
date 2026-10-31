## SPDX-License-Identifier: MIT
"""Load and prepare qtop configuration without runtime-global dependencies."""

import copy
import logging
import os
from ast import literal_eval
from math import isfinite

from qtop_py import fileutils
from qtop_py import yaml_parser as yaml
from qtop_py.colormap import nodestate_to_color_default, user_to_color_default
from qtop_py.constants import QTOPCONF_YAML, SYMBOL_LONG_TAIL_USER, SYMBOL_UNKNOWN_NODE_STATE
from qtop_py.errors import OperationalError


def literal(value):
    try:
        return literal_eval(value)
    except (ValueError, SyntaxError, TypeError):
        return value


def validate_config(config):
    """Check merged defaults/overrides; retain extensible backend-specific keys."""

    def require(condition, path, expected):
        if not condition:
            raise ValueError("%s: expected %s" % (path, expected))

    def integer(value, path, minimum=0):
        value = literal(value)
        require(type(value) is int and value >= minimum, path, "an integer >= %s" % minimum)

    def mapping(value, path):
        require(isinstance(value, dict), path, "a mapping")
        return value

    def text(value, path):
        require(isinstance(value, str) and bool(value.strip()), path, "nonempty text")

    mapping(config, "configuration")
    for key in ("scheduler", "savepath", "vertical_separator", "non_existent_node_symbol"):
        text(config.get(key), key)
    for key in ("transpose_wn_matrices", "fill_with_user_firstletter", "faster_xml_parsing", "overwrite_sample_file"):
        value = literal(config.get(key))
        require(type(value) in (bool, int) and value in (False, True), key, "True, False, 0 or 1")
    for key in ("vertical_separator_every_X_columns", "rem_empty_corelines", "exotic_starting_wn_nr"):
        integer(config.get(key, 0), key)
    timeout = config.get("scheduler_command_timeout", 30)
    try:
        require(not isinstance(timeout, bool) and isfinite(float(timeout)) and float(timeout) > 0, "scheduler_command_timeout", "positive finite seconds")
    except (TypeError, ValueError, OverflowError):
        raise ValueError("scheduler_command_timeout: expected positive finite seconds")
    ids = config.get("possible_ids")
    require(isinstance(ids, (str, list, tuple)) and bool(ids), "possible_ids", "a nonempty sequence of single characters")
    require(all(isinstance(symbol, str) and len(symbol) == 1 for symbol in ids), "possible_ids", "single characters")
    sorting = mapping(config.get("sorting"), "sorting")
    reverse = literal(sorting.get("reverse", False))
    require(type(reverse) in (bool, int) and reverse in (False, True), "sorting.reverse", "True, False, 0 or 1")
    order = sorting.get("user_sort")
    require(order is None or isinstance(order, list) and all(isinstance(item, str) for item in order), "sorting.user_sort", "a list of sort names or null")
    for key in ("user_color_mappings", "nodestate_color_mappings", "queue_color_mappings"):
        entries = config.get(key)
        require(isinstance(entries, list) or not entries, key, "a list of mappings")
        for entry in entries or []:
            mapping(entry, key)
            require(all(isinstance(pattern, str) and isinstance(color, str) for pattern, color in entry.items()), key, "text patterns and color names")
    parts = config.get("user_display_parts")
    allowed = ("job_accounting_summary", "workernodes_matrix", "user_accounts_pool_mappings")
    require(isinstance(parts, list) and len(parts) <= 3 and all(part in allowed for part in parts), "user_display_parts", "up to three known display sections")
    matrices = config.get("workernodes_matrix")
    require(isinstance(matrices, list) and bool(matrices), "workernodes_matrix", "a nonempty list of mappings")
    for index, entry in enumerate(matrices):
        mapping(entry, "workernodes_matrix[%s]" % index)
        require(len(entry) == 1, "workernodes_matrix[%s]" % index, "one matrix definition")
        for name, settings in entry.items():
            mapping(settings, "workernodes_matrix.%s" % name)
    labels = mapping(matrices[0].get("wn id lines"), "workernodes_matrix[0].wn id lines")
    for key in ("max_len", "min_masking_threshold", "user_cut_matrix_width"):
        integer(labels.get(key), "workernodes_matrix.wn id lines.%s" % key, 1 if key == "max_len" else 0)
    require(
        isinstance(labels.get("alt_label_colors"), list) and bool(labels["alt_label_colors"]) and all(isinstance(color, str) for color in labels["alt_label_colors"]),
        "workernodes_matrix.wn id lines.alt_label_colors",
        "a nonempty list of color names",
    )
    if "term_size" in config:
        size = literal(config["term_size"])
        require(isinstance(size, (list, tuple)) and len(size) == 2, "term_size", "[rows, columns]")
        require(all(type(literal(value)) is int and literal(value) > 0 for value in size), "term_size", "two positive integers")
    for key in ("schedulers", "signature_commands", "state_abbreviations"):
        mapping(config.get(key), key)
    for name, entries in config["schedulers"].items():
        for key, command in mapping(entries, "schedulers.%s" % name).items():
            path = "schedulers.%s.%s" % (name, key)
            text(command, path)
            require(", " in command and all(part.strip() for part in command.split(", ", 1)), path, "a filename, command pair")
    for name, command in config["signature_commands"].items():
        text(command, "signature_commands.%s" % name)
    info = config.get("extract_info")
    if info:
        mapping(info, "extract_info")
        integer(info.get("field_to_use", 5), "extract_info.field_to_use", 2)
        require(info.get("regex") is None or isinstance(info["regex"], str), "extract_info.regex", "text or null")


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
    config["ALT_LABEL_COLORS"] = yaml.fix_config_list(wn_id_lines["alt_label_colors"])
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
    config = yaml.parse(os.path.join(os.path.realpath(package_dir), QTOPCONF_YAML))
    logging.info("Default configuration dictionary loaded. Length: %s items", len(config))
    for label, directory in (("Env", system_dir), ("User", user_dir)):
        try:
            layer = yaml.parse(os.path.join(directory, QTOPCONF_YAML))
        except FileNotFoundError:
            logging.info("%s could not be found in %s/", QTOPCONF_YAML, directory)
        else:
            config.update(layer)
            logging.info("%s configuration loaded from %s/. Length: %s items", label, directory, len(layer))
    if args.CONFFILE:
        for directory in (user_dir, initial_cwd):
            try:
                custom = yaml.parse(os.path.join(directory, args.CONFFILE))
            except FileNotFoundError:
                continue
            config.update(custom)
            logging.info("Custom User configuration loaded from %s/. Length: %s items", directory, len(custom))
            break
        else:
            raise OperationalError("Custom configuration file %s was not found in %s/ or %s/." % (args.CONFFILE, user_dir, initial_cwd))
    logging.info("Updated main dictionary. Length: %s items", len(config))
    return prepare_config(config, args)
