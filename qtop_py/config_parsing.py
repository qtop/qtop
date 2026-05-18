"""Helpers for parsing qtop configuration values safely."""

import ast


def parse_config_literal(value):
    """Parse simple Python/YAML-style literals without executing code."""
    if not isinstance(value, str):
        return value

    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value


def parse_config_bool(value):
    """Parse common textual boolean values from the config file."""
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return bool(value)

    if not isinstance(value, str):
        return bool(value)

    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise ValueError("Cannot parse %r as a boolean value" % value)


def parse_config_int(value):
    """Parse integer-like config values."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(str(value).strip())
