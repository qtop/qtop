## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The scheduler core stays free of the application around it, so it can become a library.

Core: the top-level ``qtop_py/core_*.py`` modules (cluster state and its schema, input reading,
trace masking, snapshots, the command runner...), the backends in ``plugins/`` and the helpers in
``schedulers/`` except the collector. It may import only other core modules (and the
standard library or third-party packages), and must not print, touch stdout/stderr/stdin, exit,
read the environment or the home directory, or log through the root logger: logging.warning() and
friends call basicConfig() in a host application that has not set up logging, so the core uses
logging.getLogger(__name__). Configuration files, savepath, logging setup, colours
and the TUI belong to the application, which passes what the core needs.

KNOWN_VIOLATIONS is empty. A new violation fails; were one ever listed, it must stay real, so
the list can only shrink.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "qtop_py"

CORE_PACKAGES = ("qtop_py.plugins", "qtop_py.schedulers")
APP_IN_CORE_PACKAGES = {"qtop_py.schedulers.collector"}  # orchestrates a run from the configuration

SIDE_EFFECTS = {
    ("sys", "stdout"),
    ("sys", "stderr"),
    ("sys", "stdin"),
    ("sys", "exit"),
    ("os", "environ"),
    ("os", "getenv"),
    ("os", "chdir"),
    ("path", "expanduser"),
    ("path", "expandvars"),
} | {("logging", level) for level in ("debug", "info", "warning", "error", "critical", "exception", "log", "basicConfig")}

KNOWN_VIOLATIONS = set()  # empty: keep it so; a new entry needs a reason and a plan to remove it


def _module_name(path):
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


MODULES = {_module_name(path): path for path in PACKAGE.rglob("*.py")}
CORE = {
    name
    for name in MODULES
    if name.startswith("qtop_py.core_") or name in CORE_PACKAGES or (name.startswith(tuple(package + "." for package in CORE_PACKAGES)) and name not in APP_IN_CORE_PACKAGES)
}


def _imported_modules(node):
    if isinstance(node, ast.Import):
        return {alias.name for alias in node.names if alias.name.startswith("qtop_py.")}
    if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module and node.module.startswith("qtop_py"):
        return {node.module + "." + alias.name if node.module + "." + alias.name in MODULES else node.module for alias in node.names}
    return set()


def _violations(name):
    found = set()
    for node in ast.walk(ast.parse(MODULES[name].read_text(encoding="utf-8"))):
        found |= {"imports " + module for module in _imported_modules(node) if module not in CORE and module != "qtop_py"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("print", "input"):
            found.add("calls " + node.func.id)
        if isinstance(node, ast.Attribute) and isinstance(node.value, (ast.Name, ast.Attribute)):
            base = node.value.id if isinstance(node.value, ast.Name) else node.value.attr
            if (base, node.attr) in SIDE_EFFECTS:
                found.add("uses %s.%s" % (base, node.attr))
    return {(name, violation) for violation in found}


def _all_violations():
    return set().union(*(_violations(name) for name in CORE))


def test_core_modules_exist():
    assert APP_IN_CORE_PACKAGES <= set(MODULES)
    assert len([name for name in CORE if name.startswith("qtop_py.core_")]) >= 12


def test_core_adds_no_application_dependency_or_side_effect():
    new = sorted(_all_violations() - KNOWN_VIOLATIONS)
    assert not new, "core must not depend on the application; pass what it needs instead: %s" % new


def test_known_violations_are_still_real():
    gone = sorted(KNOWN_VIOLATIONS - _all_violations())
    assert not gone, "remove these from KNOWN_VIOLATIONS, they are fixed: %s" % gone


def test_the_check_sees_a_violation(tmp_path, monkeypatch):
    module = PACKAGE / "plugins" / "demo.py"
    source = module.read_text(encoding="utf-8") + "\nfrom qtop_py import configuration\nprint(configuration)\nlogging.warning(configuration)\n"
    planted = tmp_path / "demo.py"
    planted.write_text(source, encoding="utf-8")
    monkeypatch.setitem(MODULES, "qtop_py.plugins.demo", planted)
    expected = {"imports qtop_py.configuration", "calls print", "uses logging.warning"}
    assert {("qtop_py.plugins.demo", violation) for violation in expected} <= _violations("qtop_py.plugins.demo")
