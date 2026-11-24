## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Optional account enrichment, kept outside snapshot queries and rendering."""

import logging
import os
import shlex
import subprocess  # nosec B404 - getent command as an argv list, never a shell
from functools import lru_cache
from math import isfinite
from typing import Any, Dict, Sequence

from qtop_py.constants import DEFAULT_SCHEDULER_COMMAND_TIMEOUT, QTOPCONF_YAML
from qtop_py.core_errors import OperationalError
from qtop_py.core_process import communicate_with_timeout, scheduler_env
from qtop_py.core_safety import visible_text
from qtop_py.input_parsing import extract_regex_detail


def MVU_resolve_user_details(model: Any, occupancy: Any, force: bool = False) -> None:
    """Load user full names into the model, only if the accounts table is shown (or force is set)."""
    parts = model.config.get("user_display_parts", [])
    if not force and not any(
        part == "user_accounts_pool_mappings" and not model.display_overrides.get("sect_%s_off" % index, getattr(model.args, "sect_%s_off" % index, False))
        for index, part in enumerate(parts, 1)
    ):
        model.user_details = {}
        return
    model.user_details = {
        "names": get_detail_of_name(occupancy.account_jobs_table, model.config, model.args),
        "groups": get_primary_group_names(occupancy.account_jobs_table) if getattr(model.args, "SHOW_GROUPS", False) else None,
    }


def get_detail_of_name(account_jobs_table: Sequence[Sequence[Any]], config: Dict[str, Any], args: Any) -> Dict[str, str]:
    """Return each user's full name, read from getent_passwd.txt (or the file QTOPCONF_YAML names)."""
    extract_info = config.get("extract_info", None)
    if not extract_info:
        return dict()

    sep = ":"
    field_idx = int(extract_info.get("field_to_use", 5))
    regex = extract_info.get("regex", None)

    command_key = "user_details_realtime" if args.GET_GECOS else "user_details_cache"
    command = extract_info.get(command_key)
    if not isinstance(command, str) or not command.strip():
        logging.warning("No valid %s command in %s; skipping user details.", command_key, QTOPCONF_YAML)
        return {}

    try:
        if args.GET_GECOS:
            users = " ".join(shlex.quote(str(line[4])) for line in account_jobs_table if line[4] != "Totals")
            command = command % users
        user_details_command = shlex.split(command)
        if not args.GET_GECOS:
            user_details_command[-1] = os.path.expandvars(user_details_command[-1])
        timeout = float(config.get("scheduler_command_timeout", DEFAULT_SCHEDULER_COMMAND_TIMEOUT))
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("scheduler_command_timeout must be positive and finite")
        p = subprocess.Popen(  # nosec B603 # nosemgrep - shlex-split user-details command, argv list, never a shell
            user_details_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=(os.name == "posix"),
            env=scheduler_env(),
        )
        raw_output, raw_err = communicate_with_timeout(p, timeout)
        output, err = (raw_output or b"").decode("utf-8", errors="replace"), raw_err.decode("utf-8", errors="replace")
    except (OperationalError, OSError, ValueError, TypeError, IndexError, OverflowError, subprocess.TimeoutExpired) as error:
        logging.warning("Cannot collect %s command %r: %s; skipping user details.", command_key, command, error)
        return {}
    if p.returncode:
        logging.warning("%s failed (exit %s): %s; skipping user details.", command_key, p.returncode, err.strip()[:300])
        return {}
    if err:
        logging.warning("%s: %s", command_key, err.strip()[:300])

    detail_of_name = dict()
    for line in output.split("\n"):
        try:
            user, field = line.strip().split(sep)[0 : field_idx : field_idx - 1]
        except ValueError:
            continue
        else:
            try:
                detail = extract_regex_detail(regex, field)
            except (AttributeError, TypeError, ValueError):
                detail = field.strip()
            finally:
                detail_of_name[user] = visible_text(detail)
    return detail_of_name


@lru_cache(maxsize=None)
def get_primary_group_name(user: str) -> str:
    """Resolve and cache one primary group, including through NSS/LDAP."""
    try:
        import grp
        import pwd
    except ImportError:
        logging.warning("Unix group information is unavailable on this platform.")
        return ""

    try:
        return grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
    except (KeyError, OSError):
        logging.warning("Could not resolve the primary Unix group for %s.", user)
        return ""


def get_primary_group_names(account_jobs_table: Sequence[Sequence[Any]]) -> Dict[str, str]:
    """Resolve primary groups only for users present in the current snapshot."""
    users = (line[4] for line in account_jobs_table if line[4] != "Totals")
    groups = ((user, get_primary_group_name(user)) for user in users)
    return dict((user, group) for user, group in groups if group)
