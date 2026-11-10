## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Critical MVU boundaries: explicit transitions and isolated query/presentation."""

import copy
import re
import sys
from pathlib import Path

import pytest

from qtop_py import configuration, qtop, utils, yaml_parser
from qtop_py.mvu import MVU_Model, MVU_update
from qtop_py.mvu_query import MVU_query
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.snapshot import Snapshot
from qtop_py.ui import mvu_view
from qtop_py.ui.mvu_view import MVU_view
from qtop_py.ui.viewport import Viewport


def MVU_test_model(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", ["qtop", "-b", "slurm", "-F", "-o", "savepath=" + str(tmp_path)])
    args = utils.parse_qtop_cmdline_args()
    args.COLOR, args.REMAP = "OFF", False
    config, users, states = configuration.prepare_config(yaml_parser.parse(str(root / "qtopconf.yaml")), args)
    source = root / "tests/plugins/slurm_samples/basic"
    backend = SlurmBatchSystem({"squeue_file": str(source / "squeue.txt"), "sinfo_file": str(source / "sinfo.txt")}, config, args)
    snapshot = Snapshot.from_cluster_state(backend.get_cluster_state(args))
    viewport = Viewport(0, 0)
    viewport.set_term_size(40, 120)
    return MVU_Model(
        config,
        args,
        viewport,
        backend=SlurmBatchSystem,
        display_overrides={"force_names": True},
        snapshot=snapshot,
        user_to_color=users,
        nodestate_to_color=states,
        package_path=str(root),
        help_file="helpfile.txt",
    )


def test_MVU_update_returns_owned_state_and_effects_without_io(tmp_path, monkeypatch):
    model = MVU_test_model(tmp_path, monkeypatch)
    original_overrides = model.display_overrides.copy()
    original_viewport = vars(model.viewport).copy()
    next_model, effects = MVU_update(model, "t")
    assert next_model is not model
    assert next_model.display_overrides["transpose_wn_matrices"] is not model.config["transpose_wn_matrices"]
    assert effects == (("notice", "Transposing matrix..."),)
    assert model.display_overrides == original_overrides and vars(model.viewport) == original_viewport
    assert MVU_update(model, "unknown") == (model, ())
    assert MVU_update(model, "q")[1] == (("quit", None),)
    help_model, _ = MVU_update(model, "?")
    assert help_model.display_overrides["output_fp"] == model.help_file
    restored_model, _ = MVU_update(help_model, "?")
    assert "output_fp" not in restored_model.display_overrides
    assert model.display_overrides == original_overrides


SECTION_KEYS = [("1", "sect_1_off", "Job accounting summary"), ("2", "sect_2_off", "Worker Nodes occupancy"), ("3", "sect_3_off", "User accounts")]
SECTION_KEYS += [("4", "SHOW_ACCOUNT_TOTALS", "Totals")]


@pytest.mark.parametrize("key, override, gone", SECTION_KEYS)
def test_number_keys_toggle_sections(tmp_path, monkeypatch, capsys, key, override, gone):
    model = MVU_test_model(tmp_path, monkeypatch)
    hidden, effects = MVU_update(model, key)
    assert effects[0][0] == "notice" and MVU_update(hidden, key)[0].display_overrides[override] is not hidden.display_overrides[override]
    for current, shown in ((model, True), (hidden, False)):
        MVU_view(MVU_query(current), str(tmp_path), "sample.tar", "qtop.log")
        assert (gone in capsys.readouterr().out) is shown


def test_palette_key_cycles_the_shipped_palettes_and_recolours(tmp_path, monkeypatch):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.config["palettes"] = configuration.load_palettes((str(Path(__file__).resolve().parents[1]),))
    model.args.COLOR = "ON"
    configuration.validate_config(model.config)
    seen = []
    for _ in model.config["palettes"]["PALETTES"]:
        seen.append((model.display_overrides.get("palette", "default"), model.MVU_colorize("x", "Red_L")))
        model, effects = MVU_update(model, "p")
        assert effects == (("notice", "Palette: " + model.display_overrides["palette"]),)
    assert [name for name, _ in seen] == ["default", "green-monitor", "solarized-dark", "gruvbox", "okabe-ito", "monochrome"]
    assert model.display_overrides["palette"] == "default"  # wrapped around
    assert seen[0][1] == "\x1b[1;31mx\x1b[0;m" and seen[1][1] == "\x1b[1;38;5;40mx\x1b[0;m" and len({code for _, code in seen}) == 6


def test_user_palettes_override_colour_by_colour_and_are_validated(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "palettes.yaml").write_text("PALETTES:\n  - default\n  - mine\nmine:\n  Red: '0;31'\n  Blue: '0;34'\n")
    (tmp_path / "palettes.yaml").write_text("PALETTES:\n  - mine\nmine:\n  Blue: '1;38;5;33'\n")
    palettes = configuration.load_palettes((str(tmp_path / "pkg"), str(tmp_path / "missing"), str(tmp_path)))
    assert palettes == {"PALETTES": ["mine"], "mine": {"Red": "0;31", "Blue": "1;38;5;33"}}
    for broken in ({"PALETTES": ["nope"]}, {"mine": {"Red": "31m\x1b[2J"}}, {"mine": {"NoSuchColour": "1"}}):
        with pytest.raises(ValueError, match="palettes"):
            configuration._validate_palettes({"palettes": broken})


def test_watch_keys_line_stays_when_section_1_is_hidden(tmp_path, monkeypatch, capsys):
    model, _ = MVU_update(MVU_test_model(tmp_path, monkeypatch), "1")
    model.args.WATCH = True
    MVU_view(MVU_query(model), str(tmp_path), "sample.tar", "qtop.log")
    out = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    assert out.count(mvu_view.WATCH_KEYS_TEXT) == 1 and "Job accounting summary" not in out


@pytest.mark.parametrize("force_names", [False, True])
def test_MVU_query_and_view_need_no_runtime_globals_or_mutable_snapshot(tmp_path, monkeypatch, capsys, force_names):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.display_overrides["force_names"] = force_names
    original = model.snapshot.to_cluster_state()
    original_nodes = copy.deepcopy(model.snapshot.worker_nodes)
    original_viewport = vars(model.viewport).copy()
    original_config = copy.deepcopy(model.config)
    for name in ("args", "config", "dynamic_config", "scheduler", "snapshot", "cluster", "viewport", "user_to_color", "transposed_sections"):
        monkeypatch.delattr(qtop, name, raising=False)
    view_data = MVU_query(model)
    MVU_view(view_data, str(tmp_path), "sample.tar", "qtop.log")
    assert "Job accounting summary" in capsys.readouterr().out
    assert model.snapshot.to_cluster_state() == original
    assert model.snapshot.worker_nodes == original_nodes
    assert model.config == original_config
    assert vars(model.viewport) == original_viewport
    assert model.args.REMAP is False


# Running/queued counts per user; "all" adds jobs in other states (e.g. completed).
RANKING_ALL = {"amy": 5, "bob": 4, "cid": 4, "dan": 2, "eve": 2}
RANKING_RUNNING = {"amy": 0, "bob": 3, "cid": 1, "dan": 1, "eve": 1}
RANKING_QUEUED = {"amy": 1, "bob": 0, "cid": 2, "dan": 1, "eve": 1}


@pytest.mark.parametrize(
    "USER_ID_RANKING, expected",
    [
        (("all", "R", "name_desc"), ["amy", "bob", "cid", "eve", "dan"]),
        (("all", "R", "name_asc"), ["amy", "bob", "cid", "dan", "eve"]),
        (("R+Q", "R", "name_desc"), ["bob", "cid", "eve", "dan", "amy"]),
        (("R+Q", "R", "name_asc"), ["bob", "cid", "dan", "eve", "amy"]),
        (("R+Q", "name_desc"), ["cid", "bob", "eve", "dan", "amy"]),
        (("R+Q", "name_asc"), ["bob", "cid", "dan", "eve", "amy"]),
        (("all", "name_desc"), ["amy", "cid", "bob", "eve", "dan"]),
        (("name_asc",), ["amy", "bob", "cid", "dan", "eve"]),
    ],
)
def test_user_id_ranking_orders_by_counts_then_name(USER_ID_RANKING, expected):
    from qtop_py.mvu_query import rank_users

    lot = rank_users(RANKING_ALL, RANKING_RUNNING, RANKING_QUEUED, USER_ID_RANKING)

    assert [user for user, _ in lot] == expected
    assert all(count == RANKING_ALL[user] for user, count in lot)


def test_user_id_ranking_default_is_all_then_r_then_name_desc():
    from qtop_py import configuration, yaml_parser
    from qtop_py.constants import DEFAULT_USER_ID_RANKING

    config = yaml_parser.parse(str(Path(__file__).resolve().parents[1] / "qtopconf.yaml"))

    assert tuple(config["user_id_ranking"]) == DEFAULT_USER_ID_RANKING == ("all", "R", "name_desc")
    configuration._validate_user_id_ranking(config)


@pytest.mark.parametrize(
    "USER_ID_RANKING",
    [[], ["R+Q"], ["name_desc", "R"], ["R", "R", "name_asc"], ["cores", "name_asc"], ["R", "name_asc", "name_desc"], "R+Q,name_desc"],
)
def test_user_id_ranking_rejects_orders_that_are_not_total(USER_ID_RANKING):
    from qtop_py import configuration

    with pytest.raises(ValueError, match="user_id_ranking"):
        configuration._validate_user_id_ranking({"user_id_ranking": USER_ID_RANKING})


def test_rendered_matrix_does_not_depend_on_hash_seed(tmp_path):
    import os
    import subprocess

    root = Path(__file__).resolve().parents[1]
    source = root / "tests/plugins/slurm_samples/basic"
    renders = set()
    for seed in ("0", "1", "2", "3", "4"):
        env = dict(os.environ, HOME=str(tmp_path / seed), PYTHONHASHSEED=seed, PYTHONPATH=str(root))
        command = [sys.executable, "-m", "qtop_py.qtop", "-s", str(source), "-c", "OFF", "-F", "-b", "slurm"]
        output = subprocess.run(command, cwd=str(root), env=env, stdout=subprocess.PIPE, check=True, universal_newlines=True).stdout
        renders.add(re.sub(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d|Log file created in \S+", "", output))

    assert len(renders) == 1


def test_node_queue_names_are_sorted_with_unknown_last():
    from qtop_py.serialiser import GenericBatchSystem

    node = {"core_job_map": {"0": "3", "1": "1", "2": "2", "3": "9"}}
    (result,) = GenericBatchSystem.ensure_worker_nodes_have_qnames([node], ["1", "2", "3"], ["short", "long", "debug"])

    assert result["qname"] == ["debug", "long", "short", None]


@pytest.mark.parametrize(
    "config, expected",
    [
        ({}, "(ranked by all, R, name_desc)"),
        ({"user_id_ranking": ["R+Q", "name_asc"]}, "(ranked by R+Q, name_asc)"),
        ({"user_accounts_header": "sorted: {ranking} @ site"}, "sorted: all, R, name_desc @ site"),
        ({"user_accounts_header": "{0.__class__} {ranking!r}"}, "{0.__class__} {ranking!r}"),
        ({"user_accounts_header": ""}, ""),
        ({"user_accounts_header": "a\x1b[2Jb"}, "a\\x1b[2Jb"),
    ],
)
def test_user_accounts_header_expands_ranking_without_format_evaluation(config, expected):
    from qtop_py.ui.mvu_view import user_accounts_header

    assert user_accounts_header(config) == expected


@pytest.mark.parametrize("USER_ACCOUNTS_HEADER", [42, ["a"], "two\nlines", "x" * 201])
def test_user_accounts_header_rejects_non_single_line_text(USER_ACCOUNTS_HEADER):
    from qtop_py import configuration

    with pytest.raises(ValueError, match="user_accounts_header"):
        configuration._validate_user_accounts_header({"user_accounts_header": USER_ACCOUNTS_HEADER})


def test_anonymized_samples_drop_custom_user_accounts_header():
    from qtop_py import sample

    config, omitted = sample.sanitize_config({"user_accounts_header": "CSCS Alps: {ranking}"}, {kind: {} for kind in ("user", "node", "queue", "job")})

    assert "user_accounts_header" not in config
    assert "user_accounts_header" in omitted
