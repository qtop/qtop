## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""The scheduler core stays free of the application around it, so it can become a library.

Core: the backends (plugins), the scheduler helpers, cluster state and its schema, input reading,
trace masking, snapshots and the command runner. It may import only other core modules (and the
standard library or third-party packages), and must not print, touch stdout/stderr/stdin, exit,
read the environment or the home directory. Configuration files, savepath, logging setup, colours
and the TUI belong to the application, which passes what the core needs.

KNOWN_VIOLATIONS lists what is still to move. A new violation fails, and so does a listed one
that is gone, so the list only shrinks.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "qtop_py"

CORE_MODULES = {
    "qtop_py.cluster_state",
    "qtop_py.errors",
    "qtop_py.hostlist",
    "qtop_py.inputs",
    "qtop_py.process",
    "qtop_py.queue_nodes",
    "qtop_py.safety",
    "qtop_py.sample",
    "qtop_py.serialiser",
    "qtop_py.source_trace",
    "qtop_py.state_types",
    "qtop_py.yaml_parser",
}
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
}

KNOWN_VIOLATIONS = {
    # The default command timeout and the qtopconf.yaml name in its error messages.
    ("qtop_py.process", "imports qtop_py.constants"),
    # scheduler_env() builds the child environment; a library caller should pass it in.
    ("qtop_py.process", "uses os.environ"),
}


def _module_name(path):
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


MODULES = {_module_name(path): path for path in PACKAGE.rglob("*.py")}
CORE = {
    name
    for name in MODULES
    if name in CORE_MODULES or (name.startswith(tuple(package + "." for package in CORE_PACKAGES)) and name not in APP_IN_CORE_PACKAGES)
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
    assert CORE_MODULES <= set(MODULES) and APP_IN_CORE_PACKAGES <= set(MODULES)


def test_core_adds_no_application_dependency_or_side_effect():
    new = sorted(_all_violations() - KNOWN_VIOLATIONS)
    assert not new, "core must not depend on the application; pass what it needs instead: %s" % new


def test_known_violations_are_still_real():
    gone = sorted(KNOWN_VIOLATIONS - _all_violations())
    assert not gone, "remove these from KNOWN_VIOLATIONS, they are fixed: %s" % gone


def test_the_check_sees_a_violation(tmp_path, monkeypatch):
    module = PACKAGE / "plugins" / "demo.py"
    source = module.read_text(encoding="utf-8") + "\nfrom qtop_py import configuration\nprint(configuration)\n"
    planted = tmp_path / "demo.py"
    planted.write_text(source, encoding="utf-8")
    monkeypatch.setitem(MODULES, "qtop_py.plugins.demo", planted)
    assert {("qtop_py.plugins.demo", "imports qtop_py.configuration"), ("qtop_py.plugins.demo", "calls print")} <= _violations("qtop_py.plugins.demo")
