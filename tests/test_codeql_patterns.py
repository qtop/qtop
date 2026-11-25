## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Catch locally what CodeQL reports on pull requests but ruff does not.

- py/unused-import: a name imported under TYPE_CHECKING and used only in strings inside function
  bodies (local annotations, cast targets); CodeQL does not count those as uses.
- py/unused-local-variable: an unpacking whose plain names are all unused, including ``_``-prefixed
  ones, which ruff's RUF059 exempts.

Self-assignment (py/redundant-assignment) is ruff's PLW0127/PLW0128, enabled in pyproject.toml, for plain
names; ``x.attr = x.attr``, which SonarCloud reports, is checked here.
"""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCES = sorted((ROOT / "qtop_py").rglob("*.py"))


def _string_names(text):
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError:
        return set()
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _type_only_imports_used_only_in_function_bodies(tree):
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "TYPE_CHECKING" in ast.dump(node.test):
            for child in ast.walk(node):
                if isinstance(child, (ast.Import, ast.ImportFrom)):
                    imported |= {alias.asname or alias.name.split(".")[0] for alias in child.names}
    if not imported:
        return []
    in_bodies = set()
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for statement in function.body:
                in_bodies |= {id(node) for node in ast.walk(statement) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in in_bodies:
            used |= _string_names(node.value)
    return sorted(name for name in imported - used if name != "Final")


def _all_unused_unpackings(tree):
    found = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        loads = {node.id for node in ast.walk(function) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
        for node in ast.walk(function):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, (ast.Tuple, ast.List)):
                        names = [item.id for item in target.elts if isinstance(item, ast.Name) and item.id != "_"]
                        if names and not set(names) & loads:
                            found.append("line %d: %s" % (node.lineno, ", ".join(names)))
    return found


def _attribute_self_assignments(tree):
    return [
        "line %d" % node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Attribute) and ast.unparse(node.value) in map(ast.unparse, node.targets)
    ]


@pytest.mark.parametrize("path", SOURCES, ids=lambda path: str(path.relative_to(ROOT)))
def test_no_codeql_only_findings(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    assert not _type_only_imports_used_only_in_function_bodies(tree), "move the use into a signature or drop the import"
    assert not _all_unused_unpackings(tree), "index the value you need instead of unpacking unused names"
    assert not _attribute_self_assignments(tree), "drop the no-op self-assignment"


def test_the_checks_see_both_patterns():
    source = """
from typing import TYPE_CHECKING, cast
if TYPE_CHECKING:
    from x import Local
def f(value):
    _a, _b = value.split("@")
    queues: "Local" = cast("Local", value)
    value.text = value.text
    return queues
"""
    tree = ast.parse(source)
    assert _type_only_imports_used_only_in_function_bodies(tree) == ["Local"]
    assert _all_unused_unpackings(tree) == ["line 6: _a, _b"]
    assert _attribute_self_assignments(tree) == ["line 8"]
