## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Critical MVU boundaries: explicit transitions and isolated query/presentation."""

import copy
import json
import re
import sys
from pathlib import Path

import pytest

from qtop_py import configuration, qtop, user_details, utils, yaml_parser
from qtop_py.mvu import MVU_Model, MVU_update
from qtop_py.mvu_query import MVU_query
from qtop_py.plugins.slurm import SLURM_JOB_STATES, SlurmBatchSystem
from qtop_py.plugins.slurm_json import SlurmJSONBatchSystem
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
    assert help_model.display_overrides["show_help"] is True  # a flag, not a file path
    restored_model, _ = MVU_update(help_model, "?")
    assert "show_help" not in restored_model.display_overrides
    assert model.display_overrides == original_overrides


def test_MVU_update_S_toggles_job_stacking_from_the_configured_default(tmp_path, monkeypatch):
    model = MVU_test_model(tmp_path, monkeypatch)
    stacked, effects = MVU_update(model, "S")
    assert stacked.display_overrides["stack_jobs_per_node"] is True and effects == (("notice", "Stacking jobs per node"),)
    unstacked, effects = MVU_update(stacked, "S")
    assert unstacked.display_overrides["stack_jobs_per_node"] is False and effects == (("notice", "Showing jobs on their scheduler cores"),)
    assert "stack_jobs_per_node" not in model.display_overrides


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


@pytest.mark.parametrize("key, direction", [("p", 1), ("P", -1)], ids=["forward", "backward"])
def test_palette_key_cycles_the_shipped_palettes_and_recolours(tmp_path, monkeypatch, key, direction):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.config["palettes"] = configuration.load_palettes((str(Path(__file__).resolve().parents[1]),))
    model.args.COLOR = "ON"
    configuration.validate_config(model.config)
    seen = []
    for _ in model.config["palettes"]["PALETTES"]:
        seen.append((model.display_overrides.get("palette", "default"), model.MVU_colorize("x", "Red_L")))
        model, effects = MVU_update(model, key)
        assert effects == (("notice", "Palette: " + model.display_overrides["palette"]),)
    order = ["default", "green-monitor", "solarized-dark", "gruvbox", "okabe-ito", "monochrome"]
    assert [name for name, _ in seen] == [order[(i * direction) % len(order)] for i in range(len(order))]
    assert model.display_overrides["palette"] == "default"  # wrapped around
    assert seen[0][1] == "\x1b[1;31mx\x1b[0;m" and dict(seen)["green-monitor"] == "\x1b[1;38;5;40mx\x1b[0;m" and len({code for _, code in seen}) == 6


@pytest.mark.parametrize(
    "name",
    [
        "apprentice",
        "base16-dark",
        "base16-light",
        "catppuccin-frappe",
        "catppuccin-latte",
        "catppuccin-macchiato",
        "catppuccin-mocha",
        "dracula",
        "material",
        "papercolor-dark",
        "papercolor-light",
        "tender",
        "tokyo-night-dark",
        "tokyo-night-light",
    ],
)
def test_optional_palettes_render_when_enabled(tmp_path, monkeypatch, name):
    root = Path(__file__).resolve().parents[1]
    model = MVU_test_model(tmp_path, monkeypatch)
    model.config["palettes"] = configuration.load_palettes((str(root),))
    palettes = model.config["palettes"]
    assert len(palettes) == 20  # PALETTES plus 19 mappings; default needs none.
    assert name not in palettes["PALETTES"]
    model.args.COLOR = "ON"
    palettes["PALETTES"] = ["default", name]
    configuration.validate_config(model.config)
    forward, _ = MVU_update(model, "p")
    backward, _ = MVU_update(model, "P")
    assert forward.display_overrides["palette"] == backward.display_overrides["palette"] == name
    for colour, code in palettes[name].items():
        assert forward.MVU_colorize("x", colour) == "\x1b[" + code + "mx\x1b[0;m"
    assert MVU_update(forward, "p")[0].display_overrides["palette"] == "default"
    assert MVU_update(backward, "P")[0].display_overrides["palette"] == "default"


@pytest.mark.parametrize("key", ["p", "P"], ids=["forward", "backward"])
@pytest.mark.parametrize("current", [None, "removed"], ids=["single-palette", "removed-palette"])
def test_palette_rotation_handles_single_and_removed_palettes(tmp_path, monkeypatch, key, current):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.config["palettes"] = {"PALETTES": ["default"]}
    if current:
        model.display_overrides["palette"] = current
    rotated, effects = MVU_update(model, key)
    assert rotated.display_overrides["palette"] == "default"
    assert effects == (("notice", "Palette: default"),)


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


@pytest.mark.parametrize("accounts,totals", ((True, True), (True, False), (False, True), (False, False)))
@pytest.mark.parametrize("groups", (False, True))
def test_account_totals_are_an_independent_fourth_section(tmp_path, monkeypatch, capsys, accounts, totals, groups):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.args.SHOW_GROUPS = groups
    model.display_overrides.update(sect_3_off=not accounts, SHOW_ACCOUNT_TOTALS=totals)
    MVU_view(MVU_query(model), str(tmp_path), "sample.tar", "qtop.log")
    output = capsys.readouterr().out
    assert ("User accounts and pool mappings" in output) == accounts
    assert ("Account totals" in output) == totals
    assert ("Totals" in output) == totals


@pytest.mark.parametrize(
    "key,method",
    (
        ("up", "scroll_line_up"),
        ("down", "scroll_line_down"),
        ("left", "scroll_left"),
        ("right", "scroll_right"),
        ("home", "scroll_top"),
        ("end", "scroll_bottom"),
        ("pageup", "scroll_up"),
        ("pagedown", "scroll_down"),
    ),
)
def test_arrow_and_terminal_navigation_is_an_owned_viewport_update(tmp_path, monkeypatch, key, method):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.viewport.max_height, model.viewport.max_width = 200, 400
    model.viewport.v_start, model.viewport.h_start = 50, 100
    expected = copy.copy(model.viewport)
    getattr(expected, method)()
    updated, effects = MVU_update(model, key)
    assert vars(updated.viewport) == vars(expected)
    assert model.viewport.v_start == 50 and model.viewport.h_start == 100
    assert effects[0][0] == "notice"


@pytest.mark.parametrize("key,effect", (("/", "search"), (":", "line")))
def test_search_and_line_navigation_request_input_as_effects(tmp_path, monkeypatch, key, effect):
    model = MVU_test_model(tmp_path, monkeypatch)
    updated, effects = MVU_update(model, key)
    assert effects == ((effect, None),)
    assert vars(updated.viewport) == vars(model.viewport)


def test_hidden_accounts_skip_optional_name_lookups_even_when_totals_remain(tmp_path, monkeypatch):
    model = MVU_test_model(tmp_path, monkeypatch)
    model.display_overrides["sect_3_off"] = True
    monkeypatch.setattr(user_details, "get_detail_of_name", lambda *_args: pytest.fail("hidden account rows must not query user details"))
    user_details.MVU_resolve_user_details(model, MVU_query(model).occupancy)
    assert model.user_details == {}


@pytest.mark.parametrize("hidden", (None, "1", "2", "3", "4"))
def test_section_titles_keep_their_numbers_when_other_sections_are_hidden(tmp_path, monkeypatch, capsys, hidden):
    model = MVU_test_model(tmp_path, monkeypatch)
    if hidden:
        model, _ = MVU_update(model, hidden)
    MVU_view(MVU_query(model), str(tmp_path), "sample.tar", "qtop.log")
    output = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    for number, title in enumerate(("Job accounting summary", "Worker Nodes occupancy", "User accounts and pool mappings", "Account totals"), 1):
        assert ("===> %s. %s" % (number, title) in output) == (str(number) != hidden)


@pytest.mark.parametrize("backend", [SlurmBatchSystem, SlurmJSONBatchSystem], ids=["slurm", "slurm-json"])
@pytest.mark.parametrize("state", sorted(set(SLURM_JOB_STATES.values()) | {"UNKNOWN"}))
@pytest.mark.parametrize("detailed", [False, True], ids=["default-columns", "detailed-columns"])
def test_slurm_accounting_agrees_across_views(tmp_path, monkeypatch, capsys, backend, state, detailed):
    model = MVU_test_model(tmp_path, monkeypatch)
    if backend is SlurmBatchSystem:
        queue = tmp_path / "squeue.txt"
        queue.write_text("1|alice|%s|batch|0|\n" % state)
        nodes = tmp_path / "sinfo.txt"
        nodes.write_text("")
        files = {"squeue_file": str(queue), "sinfo_file": str(nodes)}
    else:
        source = Path(__file__).parent / "plugins/slurm_json_samples/v0.0.43"
        files = {}
        for key in ("jobs", "nodes", "partitions"):
            data = json.loads((source / ("slurm_" + key + ".json")).read_text())
            if key == "jobs":
                data[key] = [{"job_id": 1, "user_name": "alice", "job_state": [state], "partition": "batch", "cpus": 0, "nodes": ""}]
            path = tmp_path / (key + ".json")
            path.write_text(json.dumps(data))
            files[key + "_json"] = str(path)
    system = backend(files, model.config, model.args)
    model.backend = backend
    model.scheduler = "slurm"
    cluster_state = system.get_cluster_state(model.args)
    bucket = "running" if state == "R" else "queued" if state in {"PD", "RD", "RH", "RF", "RQ", "SE"} else "other"
    assert cluster_state["jobs"][0]["state"] == state
    assert cluster_state["jobs"][0]["accounting_state"] == bucket
    model.snapshot = Snapshot.from_cluster_state(cluster_state)
    expected = [int(bucket == name) for name in ("running", "queued", "other")]
    assert [model.snapshot.total_running_jobs, model.snapshot.total_queued_jobs] == expected[:2]
    original_mapping = dict(model.config["state_abbreviations"]["slurm"])
    if detailed:
        columns = ["running_of_user", "queued_of_user", "hold_of_user", "configuring_of_user", "special_exit_of_user", "other_of_user"]
        model.config["accounts_states"] = columns
        model.config["summary_states"] = columns
        category = original_mapping.get(state, "unknown_of_user")
        expected = [int(category == name) for name in columns[:-1]]
        expected.append(1 - sum(expected))
    view = MVU_query(model)
    MVU_view(view, str(tmp_path), "sample.tar", "qtop.log")
    output = capsys.readouterr().out
    assert "1=" + "+".join(map(str, expected)) + " jobs" in output
    for user in ("alice", "Totals"):
        line = next(line for line in output.splitlines() if re.search(r"\] " + user + r"\s+\|", line))
        assert list(map(int, line.split("|")[1].split())) == [1] + expected
    assert view.occupancy.account_jobs_table[0][3] == 1
    assert model.config["state_abbreviations"]["slurm"] == original_mapping
    # Portable capture/replay keeps the canonical accounting bucket and raw state.
    assert Snapshot.from_cluster_state(model.snapshot.to_cluster_state()).total_queued_jobs == model.snapshot.total_queued_jobs


def test_snapshot_is_complete_when_built_and_display_state_leaves_the_export_alone():
    state = {"jobs": [{"id": "1", "user": "alice", "state": "R", "queue": "q"}], "nodes": [], "queues": []}
    snapshot = Snapshot.from_cluster_state(state, display_state=lambda job_state: "shown-" + job_state, collection=(3.0, 1.5, "late"))
    assert snapshot.jobs_dict["1"].job_state == "shown-R"
    assert snapshot.to_cluster_state()["jobs"][0]["state"] == "R"
    assert (snapshot._collection_age, snapshot._collection_span, snapshot._collection_error) == (3.0, 1.5, "late")
    assert not hasattr(Snapshot.from_cluster_state(state), "_collection_age")


def test_MVU_update_records_the_terminal_without_effects(tmp_path, monkeypatch):
    from qtop_py.mvu import TerminalInfo

    model = MVU_test_model(tmp_path, monkeypatch)
    info = TerminalInfo("2026-01-02 03:04:05", True, False)
    next_model, effects = MVU_update(model, "terminal", info)
    assert next_model.terminal == info and effects == ()
    assert model.terminal is None


def test_view_renders_the_same_text_from_the_same_model_whatever_the_clock_or_terminal(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace

    from qtop_py.mvu import TerminalInfo

    model, _ = MVU_update(MVU_test_model(tmp_path, monkeypatch), "terminal", TerminalInfo("2026-01-02 03:04:05", False, False))
    outputs = []
    for term, tty in (("xterm-256color", True), ("linux", False)):
        monkeypatch.setenv("TERM", term)
        monkeypatch.setattr(sys, "__stdout__", SimpleNamespace(isatty=lambda tty=tty: tty))
        MVU_view(MVU_query(model), str(tmp_path), "sample.tar", "qtop.log")
        outputs.append(capsys.readouterr().out)
    assert outputs[0] == outputs[1]
    assert "2026-01-02 03:04:05" in outputs[0]
