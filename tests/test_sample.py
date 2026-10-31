## SPDX-License-Identifier: MIT

import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import sample, yaml_parser

ROOT = Path(__file__).resolve().parents[1]


def run_cli(home, *args, extra_env=None):
    home.mkdir(exist_ok=True)
    env = dict(os.environ, HOME=str(home), USER="private_operator", PYTHONHASHSEED="488", PYTHONPATH=str(ROOT), QTOP_SCHEDULER="nonsense")
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, "-m", "qtop_py.qtop"] + list(args), cwd=str(home), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=20
    )


@pytest.mark.parametrize(
    "backend,files",
    (
        ("slurm", {"squeue_file": "squeue.txt", "sinfo_file": "sinfo.txt"}),
        ("pbs", {"qstat_file": "qstat.txt", "qstatq_file": "qstat_q.txt", "pbsnodes_file": "pbsnodes_a.txt"}),
        ("oar", {"oarstat_file": "oarstat.txt", "oarnodes_s_file": "oarnodes_s_Y.txt", "oarnodes_y_file": "oarnodes_Y.txt"}),
        ("sge", {"sge_file": "qstat.F.xml.stdout"}),
    ),
)
def test_source_trace_archives_preserve_private_input_and_replay_masked_formats(tmp_path, backend, files):
    source = ROOT / "tests/plugins/slurm_samples/basic" if backend == "slurm" else ROOT / "qtop_py/contrib"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    originals = {key: (source / name).read_bytes() for key, name in files.items()}
    for key, name in files.items():
        (inputs / name).write_bytes(originals[key])
    output = tmp_path / "output"
    capture = run_cli(tmp_path / "home", "-b", backend, "-s", str(inputs), "-L", "-A", "-e", "--masked-traces", "--raw-companion", "-E", "-O", "-o", "savepath=" + str(output))
    assert capture.returncode == 0, capture.stderr
    raw = next(output.glob("qtop_raw_traces_*.tar"))
    assert raw.stat().st_mode & 0o777 == 0o600
    assert "PRIVATE unmasked scheduler traces:" in capture.stderr
    with tarfile.open(str(raw)) as archive:
        assert set(archive.getnames()) == set(files)
        for key, original in originals.items():
            assert archive.extractfile(key).read() == original
            assert archive.getmember(key).uname == ""
    masked_inputs = tmp_path / "masked"
    masked_inputs.mkdir()
    with tarfile.open(str(next(output.glob("qtop_sample_anonymous*.tar")))) as archive:
        report = json.load(archive.extractfile("masked-traces/report.json"))
        assert report["omitted"] == []
        assert set(report["included"]) == set(files)
        assert not any(name.startswith("qtop_raw") or name in files.values() for name in archive.getnames())
        for name in files.values():
            (masked_inputs / name).write_bytes(archive.extractfile("masked-traces/" + name).read())
        expected = json.load(archive.extractfile(sample.SNAPSHOT_NAME))["state"]
    replay_output = tmp_path / "replayed"
    replay = run_cli(tmp_path / "replay_home", "-b", backend, "-s", str(masked_inputs), "-E", "-O", "-o", "savepath=" + str(replay_output))
    assert replay.returncode == 0, replay.stderr
    actual = json.loads(next(replay_output.glob("qtop_json_*.json")).read_text())
    # SGE synthesizes the literal Pending queue; it is not a source XML field.
    if backend == "sge":
        expected_jobs = {job["id"]: job for job in expected["jobs"]}
        for job in actual["jobs"]:
            if job["queue"] == "Pending":
                job["queue"] = expected_jobs[job["id"]]["queue"]
    assert actual["jobs"] == expected["jobs"]
    assert {node["domainname"]: (node["np"], node["core_job_map"]) for node in actual["nodes"]} == {
        node["domainname"]: (node["np"], node["core_job_map"]) for node in expected["nodes"]
    }


@pytest.mark.parametrize("flag", ("--raw-companion", "--masked-traces"))
def test_source_trace_flags_require_anonymous_capture(tmp_path, flag):
    result = run_cli(tmp_path / "home", flag)
    assert result.returncode != 0
    assert "require -L -A -e" in result.stderr


def test_malformed_source_remains_in_private_companion_and_is_omitted_from_masked_archive(tmp_path):
    inputs = tmp_path / "inputs"
    shutil.copytree(str(ROOT / "tests/plugins/slurm_samples/basic"), str(inputs))
    with (inputs / "squeue.txt").open("a") as stream:
        stream.write("private_alice private_node secret_partition malformed\n")
    original = (inputs / "squeue.txt").read_bytes()
    output = tmp_path / "output"
    result = run_cli(tmp_path / "home", "-b", "slurm", "-s", str(inputs), "-L", "-A", "-e", "--masked-traces", "--raw-companion", "-O", "-o", "savepath=" + str(output))
    assert result.returncode == 0, result.stderr
    with tarfile.open(str(next(output.glob("qtop_raw_traces_*.tar")))) as archive:
        assert archive.extractfile("squeue_file").read() == original
    with tarfile.open(str(next(output.glob("qtop_sample_anonymous*.tar")))) as archive:
        report = json.load(archive.extractfile("masked-traces/report.json"))
        assert report["omitted"] == ["squeue_file"]
        assert "masked-traces/squeue.txt" not in archive.getnames()
        for member in archive.getmembers():
            assert b"private_alice" not in archive.extractfile(member).read()


def test_raw_companion_survives_parser_failure(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    broken = b"<job_info>private_parser_failure"
    (inputs / "qstat.F.xml.stdout").write_bytes(broken)
    output = tmp_path / "output"
    result = run_cli(tmp_path / "home", "-b", "sge", "-s", str(inputs), "-L", "-A", "-e", "--raw-companion", "-O", "-o", "savepath=" + str(output))
    assert result.returncode != 0
    with tarfile.open(str(next(output.glob("qtop_raw_traces_*.tar")))) as archive:
        assert archive.extractfile("sge_file").read() == broken


def test_masked_details_and_array_ids_keep_source_structure():
    from qtop_py.plugins.slurm import SlurmBatchSystem
    from qtop_py.source_trace import TraceMasker

    mask = TraceMasker({"job": {"42": "100000"}, "node": {"node001": "n_anon_wn_0", "node002": "n_anon_wn_1"}})
    assert mask("42_[0-3%2]", "job") == "100000_[0-3%2]"
    assert mask("42_1", "job") == "100000_1"
    assert mask("42+1", "job") == "100000+1"
    backend = SlurmBatchSystem({}, {}, SimpleNamespace(ANONYMIZE=False))
    _, text = backend.mask_source_trace("scontrol_file", "JobId=42 UserId=private_alice(999)\n   Nodes=node[001-002] CPU_IDs=0-3,7 Mem=12 GRES=gpu:h100:2\n", mask)
    assert "Nodes=n_anon_wn_0,n_anon_wn_1 CPU_IDs=0-3,7 Mem=12" in text
    assert "JobId=100000" in text
    assert "private_alice" not in text and "999" not in text and "h100" not in text


def test_json_traces_redact_unused_fields_and_keep_native_identifiers(tmp_path):
    from qtop_py.plugins.pbs import PBSBatchSystem
    from qtop_py.source_trace import TraceMasker

    backend = PBSBatchSystem({}, {}, SimpleNamespace(ANONYMIZE=False))
    mask = TraceMasker({"job": {"17": "100000"}, "user": {"alice": "u_anon_user_0"}, "queue": {"batch": "q_anon_q_0"}})
    jobs = {
        "Jobs": {"17.private-server": {"Job_Owner": "alice@private-host", "Job_Name": "private-name", "queue": "batch", "job_state": "R", "secret": {"private-label": 7654321}}}
    }
    _, text = backend.mask_source_trace("qstat_file", json.dumps(jobs), mask)
    assert "private" not in text and "7654321" not in text and "alice" not in text
    result = json.loads(text)["Jobs"]["100000.server"]
    assert result["queue"] == "q_anon_q_0" and result["job_state"] == "R"
    queues = {"Queue": {"batch": {"state_count": "Running:1 Queued:2 Held:0", "enabled": True, "started": "True", "max_run": 12, "secret": "private-value"}}}
    _, text = backend.mask_source_trace("qstatq_file", json.dumps(queues), mask)
    assert "private" not in text
    assert json.loads(text)["Queue"]["q_anon_q_0"]["state_count"] == "Running:1 Queued:2 Held:0"
    queues["Queue"]["batch"]["enabled"] = {"private-key": "private-value"}
    with pytest.raises(ValueError):
        backend.mask_source_trace("qstatq_file", json.dumps(queues), mask)


@pytest.mark.parametrize("payload", ("<job_info><private_tag>secret</private_tag></job_info>", '<!DOCTYPE job_info [<!ENTITY secret "private">]><job_info>&secret;</job_info>'))
def test_unsupported_xml_traces_are_omitted_without_exception_details(tmp_path, payload):
    from qtop_py.plugins.sge import SGEBatchSystem
    from qtop_py.source_trace import save_masked_traces

    path = tmp_path / "private.xml"
    path.write_text(payload)
    archive = tmp_path / "masked.tar"
    with tarfile.open(str(archive), "w") as stream:
        save_masked_traces(stream, SGEBatchSystem({}, {}, SimpleNamespace(ANONYMIZE=False)), {"sge_file": str(path)}, {})
    with tarfile.open(str(archive)) as stream:
        assert stream.getnames() == ["masked-traces/report.json"]
        report = stream.extractfile("masked-traces/report.json").read()
        assert b"private" not in report and b"secret" not in report
        assert json.loads(report)["omitted"] == ["sge_file"]


@pytest.mark.parametrize("backend", ("slurm", "pbs", "sge", "oar"))
@pytest.mark.parametrize("extended", (False, True))
def test_anonymous_archive_reproduces_state_and_rendering_without_originals(tmp_path, backend, extended):
    source = ROOT / "tests/plugins/slurm_samples/basic" if backend == "slurm" else ROOT / "qtop_py/contrib"
    inputs = tmp_path / "private_inputs"
    shutil.copytree(str(source), str(inputs))
    if backend == "slurm":
        with (inputs / "squeue.txt").open("a") as stream:
            stream.write("\nprivate_alice private_node secret_partition malformed\n")
    output = tmp_path / "private_output"
    output.mkdir()
    capture = run_cli(
        tmp_path / "capture_home",
        "-b",
        backend,
        "-s",
        str(inputs),
        "-LL" if extended else "-L",
        "-A",
        "-e",
        "--queue-nodes",
        "-E",
        "-O",
        "-F",
        "-T",
        "-r",
        "-c",
        "OFF",
        "-o",
        "sorting={'user_sort':['sort by core occupancy'],'reverse':True}",
        "-o",
        "savepath=" + str(output),
    )
    assert capture.returncode == 0, capture.stderr
    archive = next(output.glob("*.tar"))
    assert "private_operator" not in archive.name
    with tarfile.open(str(archive)) as saved:
        snapshot = json.load(saved.extractfile(sample.SNAPSHOT_NAME))
        names = saved.getnames()
        assert "qtop.log" not in names
        assert "qtopconf.yaml" not in names
        assert "qtop-effective-config.json" not in names
        assert ("helpfile.txt" in names) == extended
        assert bool("qtop_py/qtop.py" in names) == extended
        assert all(member.uid == 0 and member.gid == 0 and not member.uname and not member.gname for member in saved.getmembers())
        screen = saved.extractfile(next(name for name in names if name.endswith(".out"))).read().decode()
        for name in names:
            if name.endswith(".py"):
                continue
            data = saved.extractfile(name).read().decode()
            for private in ("private_alice", "private_node", "secret_partition", "private_operator", str(inputs), str(output)):
                assert private not in data
    assert json.loads(next(output.glob("qtop_json_*.json")).read_text()) == snapshot["state"]
    shutil.rmtree(str(inputs))
    replay_home = tmp_path / "replay_home"
    (replay_home / ".local/qtop").mkdir(parents=True)
    (replay_home / ".local/qtop/qtopconf.yaml").write_text("invalid configuration\n")
    replay_output = tmp_path / "replayed"
    replay_output.mkdir()
    replay = run_cli(replay_home, "--load-sample", str(archive), "-E", "-O", "-o", "savepath=" + str(replay_output))
    assert replay.returncode == 0, replay.stderr
    assert json.loads(next(replay_output.glob("qtop_json_*.json")).read_text()) == snapshot["state"]
    replay_screen = next(replay_output.glob("qtop_fullview_*.out")).read_text()

    # The wall-clock header changes; all cluster-dependent output must agree.
    def normalize(text):
        return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "TIME", text.replace("\r", "\n"))

    assert normalize(replay_screen) == normalize(screen)


def test_anonymization_preserves_array_job_links_and_removes_extra_node_fields():
    state = {
        "jobs": [{"id": "42", "user": "alice", "queue": "private", "state": "R"}],
        "queues": [{"name": "private", "running": 1, "queued": 0, "limit": "0", "state": "E"}],
        "nodes": [{"domainname": "private-node", "state": "-", "np": 2, "qname": ["private"], "core_job_map": {0: "42[7]"}, "comment": "private secret"}],
    }
    masked, mappings = sample.anonymize_state(state)
    assert masked["nodes"][0]["core_job_map"]["0"] == masked["jobs"][0]["id"] + "[7]"
    assert masked["jobs"][0]["queue"] == masked["queues"][0]["name"] == masked["nodes"][0]["qname"][0]
    assert "comment" not in masked["nodes"][0]
    assert state["jobs"][0]["user"] == "alice"
    assert mappings["user"]["alice"] == masked["jobs"][0]["user"]


@pytest.mark.parametrize("kind", ("missing", "invalid-json", "version", "symlink", "oversize"))
def test_invalid_snapshot_has_actionable_error(tmp_path, kind):
    archive = tmp_path / "broken.tar"
    data = b"bad json" if kind == "invalid-json" else b'{"format_version": 999}'
    with tarfile.open(str(archive), "w") as saved:
        member = tarfile.TarInfo("other.json" if kind == "missing" else sample.SNAPSHOT_NAME)
        if kind == "symlink":
            member.type = tarfile.SYMTYPE
            member.linkname = "/etc/passwd"
        elif kind == "oversize":
            member.type = tarfile.DIRTYPE
            member.size = 65 * 1024 * 1024
        else:
            member.size = len(data)
        saved.addfile(member, io.BytesIO(data) if member.isfile() else None)
    result = run_cli(tmp_path / "home", "--load-sample", str(archive), "-O")
    assert result.returncode == 1
    assert "Cannot load support snapshot" in result.stderr
    assert "Traceback" not in result.stderr


def test_config_masking_translates_regexes_without_changing_control_strings():
    defaults = yaml_parser.parse(str(ROOT / "qtopconf.yaml"))
    config = dict(defaults)
    config.update(
        {
            "savepath": "/private/place",
            "extract_info": {"user_details_cache": "private command"},
            "user_color_mappings": [{"R|alice": "Red"}],
            "filtering": [{"include_name_patterns": ["^private-"]}, {"include_queues": ["^state$"]}],
            "highlight": [{"or_include_user_pat": ["R|alice"]}],
        }
    )
    mappings = {"user": {"R": "u_anon_user_0", "alice": "u_anon_user_1"}, "node": {"private-node": "n_anon_wn_0"}, "queue": {"state": "q_anon_q_0"}}
    sanitized, omitted = sample.sanitize_config(config, mappings)
    assert sanitized["state_abbreviations"] == defaults["state_abbreviations"]
    assert sanitized["sorting"] == defaults["sorting"]
    assert sanitized["extract_info"] is None
    assert sanitized["savepath"] == defaults["savepath"]
    assert "extract_info" in omitted
    assert sanitized["filtering"] == [{"include_name_patterns": ["^n_anon_wn_0$"]}, {"include_queues": ["q_anon_q_0"]}]
    assert sanitized["highlight"] == [{"or_include_user_pat": ["^u_anon_user_0$", "^u_anon_user_1$"]}]
    assert sanitized["user_color_mappings"] == [{"^u_anon_user_0$": "Red"}, {"^u_anon_user_1$": "Red"}]
    assert config["savepath"] == "/private/place"


def test_loading_disables_lookup_commands_from_archive(tmp_path):
    archive = tmp_path / "sample.tar"
    with tarfile.open(str(archive), "w") as saved:
        sample.add_json(
            saved,
            sample.SNAPSHOT_NAME,
            {
                "format_version": 1,
                "scheduler": "slurm",
                "state": {"jobs": [], "nodes": [], "queues": []},
                "config": {"extract_info": {"user_details_cache": "private command"}},
                "options": {"GET_GECOS": True, "SHOW_GROUPS": True},
            },
        )
    loaded = sample.load_snapshot(str(archive))
    assert loaded["config"]["extract_info"] is None
    assert loaded["options"]["GET_GECOS"] is False
    assert loaded["options"]["SHOW_GROUPS"] is False


def test_live_capture_replays_without_pid_filename_repair(tmp_path):
    commands = tmp_path / "bin"
    commands.mkdir()
    fixture = ROOT / "tests/plugins/slurm_samples/basic"
    for command, filename in (("squeue", "squeue.txt"), ("sinfo", "sinfo.txt")):
        executable = commands / command
        executable.write_text("#!/bin/sh\ncat " + shlex.quote(str(fixture / filename)) + "\n")
        executable.chmod(0o755)
    output = tmp_path / "capture"
    output.mkdir()
    capture = run_cli(
        tmp_path / "capture_home",
        "-b",
        "slurm",
        "-L",
        "-A",
        "-e",
        "-E",
        "-O",
        "-o",
        "savepath=" + str(output),
        extra_env={"PATH": str(commands) + os.pathsep + os.environ.get("PATH", "")},
    )
    assert capture.returncode == 0, capture.stderr
    expected = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    archive = next(output.glob("*.tar"))
    shutil.rmtree(str(commands))
    replayed = tmp_path / "replayed"
    replayed.mkdir()
    replay = run_cli(tmp_path / "replay_home", "--load-sample", str(archive), "-E", "-O", "-o", "savepath=" + str(replayed))
    assert replay.returncode == 0, replay.stderr
    assert json.loads(next(replayed.glob("qtop_json_*.json")).read_text()) == expected


@pytest.mark.parametrize("anonymous", [False, True])
@pytest.mark.parametrize("sample_name", ["cpu_allocations", "arrays_and_het"])
def test_slurm_cpu_details_and_resources_survive_snapshot_replay(tmp_path, anonymous, sample_name):
    source = ROOT / "tests/plugins/slurm_samples" / sample_name
    output = tmp_path / "captured"
    output.mkdir()
    flags = ["-A", "-e"] if anonymous else []
    captured = run_cli(tmp_path / "capture_home", "-b", "slurm", "-s", str(source), "-L", "-E", "-O", "-o", "savepath=" + str(output), *flags)
    assert captured.returncode == 0, captured.stderr
    expected = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert expected["nodes"][0]["cpu_allocation_source"] == "slurm"
    if sample_name == "cpu_allocations":
        assert any(queue["state"] == "drain" for queue in expected["queues"])
    assert ("slurm_tres_alloc" in expected["jobs"][0]) is not anonymous
    archive = next(output.glob("*.tar"))
    replayed = tmp_path / "replayed"
    replayed.mkdir()
    if anonymous:
        replay_args = ["--load-sample", str(archive)]
    else:
        inputs = tmp_path / "inputs"
        inputs.mkdir()
        with tarfile.open(str(archive)) as stream:
            for filename in ("squeue.txt", "sinfo.txt", "scontrol.txt"):
                with stream.extractfile(filename) as member:
                    (inputs / filename).write_bytes(member.read())
        replay_args = ["-b", "slurm", "-s", str(inputs)]
    result = run_cli(tmp_path / "replay_home", *replay_args, "-E", "-O", "-o", "savepath=" + str(replayed))
    assert result.returncode == 0, result.stderr
    assert json.loads(next(replayed.glob("qtop_json_*.json")).read_text()) == expected
    assert "scheduler CPU IDs" in next(replayed.glob("qtop_fullview_*.out")).read_text()


def test_anonymized_slurm_parent_and_child_ids_remain_consistent():
    from types import SimpleNamespace

    from qtop_py.plugins.slurm import SlurmBatchSystem

    source = ROOT / "tests/plugins/slurm_samples/arrays_and_het"
    files = {name + "_file": str(source / (name + ".txt")) for name in ("squeue", "sinfo", "scontrol")}
    options = SimpleNamespace(ANONYMIZE=False)
    original = SlurmBatchSystem(files, {}, options).get_cluster_state(options)
    masked, mappings = sample.anonymize_state(original)
    array_parents = set()
    het_parents = set()
    ids = {job["id"] for job in masked["jobs"]}
    for old, job in zip(original["jobs"], masked["jobs"]):
        if "slurm_array_job_id" in job:
            parent = job["slurm_array_job_id"]
            array_parents.add(parent)
            assert parent != "101"
            assert job["id"] == parent + "_" + job["slurm_array_task_id"]
            assert job["slurm_array_task_id"] == old["slurm_array_task_id"]
        else:
            parent = job["slurm_het_job_id"]
            het_parents.add(parent)
            assert parent != "201"
            assert job["id"] == parent + "+" + job["slurm_het_job_offset"]
        assert job["id"] == mappings["job"][old["id"]]
    assert len(array_parents) == len(het_parents) == 1
    assert array_parents != het_parents
    assert all(job in ids for node in masked["nodes"] for job in node["core_job_map"].values())
    assert sample.validate_cluster_state(masked) == masked


@pytest.mark.parametrize("anonymous", [False, True])
def test_slurm_shared_cpus_and_partition_memberships_survive_cli_replay(tmp_path, anonymous):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "squeue.txt").write_text("1|alice|R|batch|4|node1\n2|bob|R|batch|4|node1\n3|carol|PD|batch,gpu|1|(Priority)\n")
    (inputs / "sinfo.txt").write_text("node1|batch|mix|4\nnode1|gpu|mix|4\n")
    output = tmp_path / "captured"
    output.mkdir()
    captured = run_cli(
        tmp_path / "capture_home", "-b", "slurm", "-s", str(inputs), "-L", "-E", "-O", "-c", "OFF", "-o", "savepath=" + str(output), *(["-A", "-e"] if anonymous else ["-e"])
    )
    assert captured.returncode == 0, captured.stderr
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert "2+1 jobs" in frame
    assert "global job counts are unique" in frame
    assert "omits 4 shared/excess CPU assignments" in frame
    assert state["nodes"][0]["state"] == "b"
    assert len(state["nodes"][0]["slurm_job_ids"]) == 2
    assert [queue["queued"] for queue in state["queues"]] == [1, 1]
    if not anonymous:
        assert "batch: 2 + 1" in frame and "gpu: 0 + 1" in frame
    else:
        replayed = tmp_path / "replayed"
        replayed.mkdir()
        result = run_cli(tmp_path / "replay_home", "--load-sample", str(next(output.glob("*.tar"))), "-E", "-O", "-c", "OFF", "-o", "savepath=" + str(replayed))
        assert result.returncode == 0, result.stderr
        assert json.loads(next(replayed.glob("qtop_json_*.json")).read_text()) == state
        replay_frame = next(replayed.glob("qtop_fullview_*.out")).read_text()
        assert "2+1 jobs" in replay_frame and "omits 4 shared/excess CPU assignments" in replay_frame
