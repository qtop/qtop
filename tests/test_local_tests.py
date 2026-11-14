# Copyright (c) 2026 qtop contributors
# SPDX-License-Identifier: MIT
import subprocess

from tools.local_tests import snapshot_checkout


def test_native_snapshot_includes_staged_unstaged_deleted_and_untracked_files(tmp_path):
    source = tmp_path / "source with spaces"
    source.mkdir()
    subprocess.check_call(["git", "init", str(source)])
    (source / "staged.txt").write_text("before")
    (source / "unstaged.txt").write_text("before")
    (source / "deleted.txt").write_text("before")
    subprocess.check_call(["git", "add", "."], cwd=str(source))
    subprocess.check_call(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "fixture"], cwd=str(source))
    (source / "staged.txt").write_text("staged")
    subprocess.check_call(["git", "add", "staged.txt"], cwd=str(source))
    (source / "unstaged.txt").write_text("unstaged")
    (source / "deleted.txt").unlink()
    (source / "new file.txt").write_text("new")
    (source / ".gitignore").write_text("secret.env\n")
    (source / "secret.env").write_text("excluded")
    destination = tmp_path / "snapshot"
    manifest = snapshot_checkout(source, destination)
    assert (destination / "staged.txt").read_text() == "staged"
    assert (destination / "unstaged.txt").read_text() == "unstaged"
    assert not (destination / "deleted.txt").exists()
    assert (destination / "new file.txt").read_text() == "new"
    assert not (destination / "secret.env").exists()
    assert len(manifest["working_tree_sha256"]) == 64
    assert "new file.txt" in manifest["source_status"]
