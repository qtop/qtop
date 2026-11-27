## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Optional account enrichment, kept outside snapshot queries and rendering."""

import logging
import os
import shlex
import subprocess  # nosec B404 - only for TimeoutExpired; core_process starts the command
from functools import lru_cache
from typing import Any, Dict, Sequence

from qtop_py.constants import QTOPCONF_YAML
from qtop_py.core_errors import OperationalError
from qtop_py.core_process import DEFAULT_SCHEDULER_COMMAND_TIMEOUT, communicate_with_timeout, positive_timeout, start_command
from qtop_py.core_safety import visible_text
from qtop_py.input_parsing import extract_regex_detail

logger = logging.getLogger(__name__)


def user_detail_option(args: Any, name: str) -> bool:
    """Whether GET_GECOS or SHOW_GROUPS is on; never while saving an anonymized sample (-A -L), which must hold no real names."""
    masked_sample = getattr(args, "SAMPLE", False) and getattr(args, "ANONYMIZE", False) and not getattr(args, "load_sample", None)
    return bool(getattr(args, name, False)) and not masked_sample


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
        "groups": get_primary_group_names(occupancy.account_jobs_table) if user_detail_option(model.args, "SHOW_GROUPS") else None,
    }


def get_detail_of_name(account_jobs_table: Sequence[Sequence[Any]], config: Dict[str, Any], args: Any) -> Dict[str, str]:
    """Return each user's full name, read from getent_passwd.txt (or the file QTOPCONF_YAML names)."""
    extract_info = config.get("extract_info", None)
    if not extract_info:
        return dict()

    sep = ":"
    field_idx = int(extract_info.get("field_to_use", 5))
    regex = extract_info.get("regex", None)

    realtime = user_detail_option(args, "GET_GECOS")
    command_key = "user_details_realtime" if realtime else "user_details_cache"
    command = extract_info.get(command_key)
    if not isinstance(command, str) or not command.strip():
        logger.warning("No valid %s command in %s; skipping user details.", command_key, QTOPCONF_YAML)
        return {}

    try:
        if realtime:
            users = " ".join(shlex.quote(str(line[4])) for line in account_jobs_table if line[4] != "Totals")
            command = command % users
        user_details_command = shlex.split(command)
        if not realtime:
            user_details_command[-1] = os.path.expandvars(user_details_command[-1])
        timeout = positive_timeout(config.get("scheduler_command_timeout", DEFAULT_SCHEDULER_COMMAND_TIMEOUT))
        p = start_command(user_details_command, os.environ)
        raw_output, raw_err = communicate_with_timeout(p, timeout)
        output, err = (raw_output or b"").decode("utf-8", errors="replace"), raw_err.decode("utf-8", errors="replace")
    except (OperationalError, OSError, ValueError, TypeError, IndexError, OverflowError, subprocess.TimeoutExpired) as error:
        logger.warning("Cannot collect %s command %r: %s; skipping user details.", command_key, command, error)
        return {}
    if p.returncode:
        logger.warning("%s failed (exit %s): %s; skipping user details.", command_key, p.returncode, err.strip()[:300])
        return {}
    if err:
        logger.warning("%s: %s", command_key, err.strip()[:300])

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
        logger.warning("Unix group information is unavailable on this platform.")
        return ""

    try:
        return grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
    except (KeyError, OSError):
        logger.warning("Could not resolve the primary Unix group for %s.", user)
        return ""


def get_primary_group_names(account_jobs_table: Sequence[Sequence[Any]]) -> Dict[str, str]:
    """Resolve primary groups only for users present in the current snapshot."""
    users = (line[4] for line in account_jobs_table if line[4] != "Totals")
    groups = ((user, get_primary_group_name(user)) for user in users)
    return dict((user, group) for user, group in groups if group)
