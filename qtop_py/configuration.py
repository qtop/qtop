## SPDX-License-Identifier: MIT
"""Validate configuration boundaries without adding a YAML dependency."""

from ast import literal_eval
from math import isfinite


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
