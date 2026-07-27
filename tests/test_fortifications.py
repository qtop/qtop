from tools import fortifications


def test_find_star_imports_reports_wildcard_imports(monkeypatch, tmp_path):
    source = tmp_path / "qtop_py" / "bad_import.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import *\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == ["qtop_py/bad_import.py:1 wildcard import is not allowed"]


def test_find_star_imports_accepts_explicit_imports(monkeypatch, tmp_path):
    source = tmp_path / "qtop_py" / "good_import.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import color_to_code\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == []


def test_find_star_imports_ignores_tests(monkeypatch, tmp_path):
    source = tmp_path / "tests" / "helper.py"
    source.parent.mkdir()
    source.write_text("from qtop_py.colormap import *\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    assert fortifications.find_star_imports() == []


def test_find_star_imports_reports_syntax_errors(monkeypatch, tmp_path):
    source = tmp_path / "tools" / "broken.py"
    source.parent.mkdir()
    source.write_text("def broken(:\n", encoding="utf-8")

    monkeypatch.setattr(fortifications, "ROOT", tmp_path)
    monkeypatch.setattr(fortifications, "iter_python_files", lambda: [source])

    problems = fortifications.find_star_imports()
    assert len(problems) == 1
    assert problems[0].startswith("tools/broken.py:1 syntax error while scanning imports: ")
