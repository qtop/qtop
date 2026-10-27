import os
import re
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree as etree

import pytest

ROOT = Path(__file__).resolve().parents[1]


def run_cli(tmp_path, module, *args, extra_env=None):
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    env.pop("QTOP_SCHEDULER", None)
    env.update(extra_env or {})
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    return subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=str(ROOT),
        env=env,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )


@pytest.mark.parametrize(
    "args, expected",
    (
        (("-d",), "No scheduler could be auto-detected"),
        (("-bb",), "Selected scheduler system not supported"),
        (("-b", "not-a-scheduler"), "Selected scheduler system not supported"),
    ),
)
@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
def test_expected_cli_scheduler_errors_are_concise(tmp_path, module, args, expected):
    result = run_cli(tmp_path, module, *args)
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert expected in output
    assert "Traceback" not in output
    assert "SchedulerNotSpecified" not in output
    assert "InvalidScheduler" not in output
    assert "SyntaxWarning" not in output


def test_packaging_entry_points_use_cli_wrapper():
    packaging_config = (ROOT / "pyproject.toml").read_text()

    assert "[project.scripts]" in packaging_config
    assert 'qtop = "qtop_py.qtop:cli_main"' in packaging_config


@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
def test_no_scheduler_message_is_not_duplicated(tmp_path, module):
    result = run_cli(tmp_path, module, extra_env={"QTOP_SCHEDULER": "nonsense"})
    output = result.stdout + result.stderr

    assert result.returncode == 1
    assert output.count("No suitable scheduler was found") == 1
    assert "Traceback" not in output


@pytest.mark.parametrize("module", ("qtop_py.cli", "qtop_py.qtop"))
@pytest.mark.parametrize("failure", ("source", "trace", "config", "empty-config", "invalid-config", "option", "term-size", "log-directory"))
def test_operational_failures_are_actionable(tmp_path, module, failure):
    args = ["-b", "slurm"]
    expected = ""
    if failure == "source":
        args += ["-s", str(tmp_path / "missing-source")]
        expected = "missing-source"
    elif failure == "trace":
        args += ["-s", str(tmp_path)]
        expected = "Scheduler input file"
    elif failure == "config":
        args += ["-f", str(tmp_path / "missing.yaml")]
        expected = "missing.yaml"
    elif failure in ("empty-config", "invalid-config"):
        path = tmp_path / "custom.yaml"
        path.write_text("" if failure == "empty-config" else "sorting: wrong-type\n", encoding="utf-8")
        args += ["-f", str(path)]
        expected = "empty" if failure == "empty-config" else "Invalid configuration"
    elif failure == "option":
        args += ["-o", "missing-equals"]
        expected = "expected KEY=VALUE"
    elif failure == "term-size":
        args += ["-o", "term_size=[0,80]"]
        expected = "two positive integers"
    else:
        # Exercise failure before logging is initialized, without chmod/root
        # behavior differences between CI and developer machines.
        (tmp_path / ".local").write_text("not a directory", encoding="utf-8")
        expected = ".local"
    result = run_cli(tmp_path, module, *args)
    assert result.returncode == 1
    assert "qtop: " in result.stderr
    assert expected in result.stderr
    assert "Traceback" not in result.stderr


def test_missing_scheduler_command_is_actionable(tmp_path):
    config = tmp_path / "commands.yaml"
    config.write_text(
        "savepath: %s\nschedulers:\n  slurm:\n    squeue_file: %%(savepath)s/squeue%%(pid)s.txt, qtop-missing-scheduler-command\n" % tmp_path,
        encoding="utf-8",
    )
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "slurm", "-f", str(config))
    assert result.returncode == 1
    assert "Cannot execute scheduler command 'qtop-missing-scheduler-command'" in result.stderr
    assert "Check PATH" in result.stderr
    assert "Traceback" not in result.stderr
    assert not list(tmp_path.glob("tmp*"))


@pytest.mark.parametrize("debug", (False, True))
def test_operational_tracebacks_are_logged_and_opt_in_on_console(tmp_path, debug):
    args = ["-b", "slurm", "-s", str(tmp_path / "missing")]
    if debug:
        args.append("-d")
    result = run_cli(tmp_path, "qtop_py.cli", *args)
    assert result.returncode == 1
    assert ("Traceback" in result.stderr) is debug
    assert result.stderr.count("qtop: ") == 1
    log = (tmp_path / ".local/qtop/logs/qtop.log").read_text(encoding="utf-8")
    assert "Traceback" in log
    assert "FileNotFoundError" in log


def test_malformed_scheduler_xml_has_concise_error(tmp_path):
    (tmp_path / "qstat.F.xml.stdout").write_text("<unfinished", encoding="utf-8")
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "sge", "-s", str(tmp_path))
    assert result.returncode == 1
    assert "Cannot parse sge scheduler input" in result.stderr
    assert str(tmp_path) in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_permission_error_restores_output(monkeypatch, capsys):
    import io

    from qtop_py import qtop

    original = sys.stdout
    redirected = io.StringIO()

    def fail():
        sys.stdout = redirected
        raise PermissionError(13, "Permission denied", "/restricted/trace")

    monkeypatch.setattr(qtop, "main", fail)
    assert qtop.cli_main() == 1
    assert sys.stdout is original
    assert redirected.closed
    assert "Permission denied (/restricted/trace)" in capsys.readouterr().err


def test_cli_does_not_hide_unexpected_programming_errors(monkeypatch):
    from qtop_py import qtop

    def fail():
        raise RuntimeError("programming defect")

    monkeypatch.setattr(qtop, "main", fail)
    with pytest.raises(RuntimeError, match="programming defect"):
        qtop.cli_main()


@pytest.mark.parametrize("exit_code", (0, 7))
def test_scheduler_command_checks_exit_status_and_cleans_up(tmp_path, exit_code):
    from qtop_py import qtop

    script = tmp_path / "command.py"
    script.write_text("import sys\nprint('usable output')\nsys.stderr.write('scheduler warning\\n')\nsys.exit(%s)\n" % exit_code, encoding="utf-8")
    output = tmp_path / "trace.txt"
    output.write_text("previous snapshot", encoding="utf-8")
    arguments = ({"trace": "%s %s" % (sys.executable, script)}, {"trace": str(output)}, "trace", str(tmp_path))
    if exit_code:
        with pytest.raises(qtop.OperationalError, match="failed \\(exit 7\\): scheduler warning"):
            qtop.execute_shell_batch_commands(*arguments)
        assert output.read_text() == "previous snapshot"
    else:
        assert qtop.execute_shell_batch_commands(*arguments) == str(output)
        assert output.read_text() == "usable output\n"
    assert {path.name for path in tmp_path.iterdir()} == {"command.py", "trace.txt"}


def test_option_value_can_contain_equals():
    from qtop_py.qtop import get_key_val_from_option_string

    assert get_key_val_from_option_string("label=a=b") == ("label", "a=b")


def test_unreadable_optional_config_is_not_ignored(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from qtop_py import qtop

    parse = qtop.yaml.parse
    optional_path = str(tmp_path / "etc" / "qtopconf.yaml")

    def deny_optional(path):
        if path == optional_path:
            raise PermissionError(13, "Permission denied", path)
        return parse(path)

    monkeypatch.setattr(qtop, "QTOPPATH", str(ROOT), raising=False)
    monkeypatch.setattr(qtop, "SYSTEMCONFDIR", str(tmp_path / "etc"))
    monkeypatch.setattr(qtop, "args", SimpleNamespace(CONFFILE=None), raising=False)
    monkeypatch.setattr(qtop.yaml, "parse", deny_optional)
    with pytest.raises(PermissionError):
        qtop.load_yaml_config()


def test_replay_configuration_failure_is_concise(tmp_path):
    config = tmp_path / "invalid.yaml"
    config.write_text("sorting: wrong-type\n", encoding="utf-8")
    result = run_cli(tmp_path, "qtop_py.cli", "-R", "0000", "-f", str(config))
    assert result.returncode == 1
    assert "Invalid configuration" in result.stderr
    assert str(config) in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.skipif(os.name != "posix", reason="Compare qtop execution with a POSIX shell")
@pytest.mark.parametrize(
    "scheduler_file, expected",
    (
        (
            "squeue_file",
            [
                "-r",
                "-h",
                "-O",
                "JobID:0|,UserName:0|,StateCompact:0|,Partition:0|,NumCPUs:0|,NodeList:0|,tres-alloc:0|,tres-per-node:0|,tres-per-task:0|,ArrayJobID:0|,ArrayTaskID:0",
            ],
        ),
        ("sinfo_file", ["-N", "-h", "-O", "NodeList:0|,Partition:0|,StateCompact:0|,CPUs:0|,Gres:0|,GresUsed:0"]),
        ("scontrol_file", ["--details", "show", "jobs"]),
    ),
)
def test_slurm_config_command_matches_shell_execution(tmp_path, monkeypatch, scheduler_file, expected):
    import json

    from qtop_py import qtop

    config = qtop.yaml.parse(str(ROOT / "qtopconf.yaml"))
    configured_command = config["schedulers"]["slurm"][scheduler_file].split(", ", 1)[1]
    executable = configured_command.split()[0]
    script = tmp_path / executable
    script.write_text("#!%s\nimport json, sys\nprint(json.dumps(sys.argv[1:]))\n" % sys.executable, encoding="utf-8")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))
    monkeypatch.setattr(qtop, "scheduler", "slurm", raising=False)
    args = type("Options", (), {"SOURCEDIR": None, "workdir": str(tmp_path)})()
    finalized = qtop.finalize_filepaths_schedulercommands(args, config)
    output_path, command = finalized[scheduler_file]
    assert command == configured_command
    shell_output = subprocess.check_output(["/bin/sh", "-c", configured_command], universal_newlines=True)
    qtop.execute_shell_batch_commands({scheduler_file: command}, {scheduler_file: output_path}, scheduler_file, str(tmp_path))
    assert json.loads(shell_output) == expected
    assert json.loads(Path(output_path).read_text()) == expected
    # Old user configs continue to produce the same scheduler arguments.
    path_template = config["schedulers"]["slurm"][scheduler_file].split(", ", 1)[0]
    legacy_command = configured_command.replace("'", "").replace("%", "%%")
    config["schedulers"]["slurm"][scheduler_file] = path_template + ", " + legacy_command
    legacy_path, legacy_command = qtop.finalize_filepaths_schedulercommands(args, config)[scheduler_file]
    qtop.execute_shell_batch_commands({scheduler_file: legacy_command}, {scheduler_file: legacy_path}, scheduler_file, str(tmp_path))
    assert json.loads(Path(legacy_path).read_text()) == expected


def test_malformed_scheduler_command_quotes_are_actionable(tmp_path):
    from qtop_py import qtop

    with pytest.raises(qtop.OperationalError, match="Check command quoting"):
        qtop.execute_shell_batch_commands({"trace": "squeue -o 'unterminated"}, {"trace": str(tmp_path / "trace.txt")}, "trace", str(tmp_path))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("flags, include_sources, include_raw", ((["-L"], False, True), (["-LL"], True, True), (["-L", "-A", "-e"], False, False)))
def test_documented_capture_and_export_modes(tmp_path, flags, include_sources, include_raw):
    import json
    import tarfile

    output = tmp_path / "capture"
    output.mkdir()
    fixture = ROOT / "tests/plugins/slurm_samples/basic"
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "slurm", "-s", str(fixture), "-F", "-E", "-O", "-o", "savepath=%s" % output, *flags)
    assert result.returncode == 0, result.stderr
    exports = list(output.glob("qtop_json_*.json"))
    assert len(exports) == 1
    state = json.loads(exports[0].read_text())
    assert {key: len(value) for key, value in state.items()} == {"jobs": 3, "nodes": 3, "queues": 2}
    assert state["jobs"][0]["id"] == ("101" if include_raw else "100000")
    assert (state["jobs"][0]["user"] == "alice") is include_raw
    assert state["nodes"][0]["core_job_map"] == dict.fromkeys(("0", "1"), state["jobs"][0]["id"])
    assert isinstance(state["queues"][0]["running"], int)
    archives = list(output.glob("qtop_sample_*.tar"))
    assert len(archives) == 1
    with tarfile.open(archives[0]) as archive:
        names = set(archive.getnames())
    assert ("qtop.log" in names) is include_raw
    assert ("qtop-snapshot.json" in names) is not include_raw
    assert any(name.startswith("qtop_fullview_") for name in names)
    assert ("squeue.txt" in names) is include_raw
    assert ("sinfo.txt" in names) is include_raw
    assert ("qtopconf.yaml" in names) is include_sources
    assert ("qtop_py/qtop.py" in names) is include_sources
    assert ("qtop_py/plugins/slurm.py" in names) is include_sources
    assert exports[0].name not in names


@pytest.mark.skipif(os.name != "posix", reason="POSIX private output directories")
@pytest.mark.parametrize("override", (False, True))
def test_cli_rejects_unsafe_savepath_before_writing(tmp_path, override):
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o777)
    if override:
        flags = ["-o", "savepath=" + str(shared)]
    else:
        config = tmp_path / "config.yaml"
        config.write_text("savepath: %s\n" % shared)
        flags = ["-f", str(config)]
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "demo", *flags)
    assert result.returncode == 1
    assert "writable by others" in result.stderr
    assert "Traceback" not in result.stderr
    assert list(shared.iterdir()) == []


@pytest.mark.parametrize("realtime", [False, True])
@pytest.mark.parametrize("command", [None, "", "/qtop-nonexistent-user-details-%s"])
def test_optional_user_details_failures_do_not_stop_rendering(tmp_path, realtime, command):
    key = "user_details_realtime" if realtime else "user_details_cache"
    fixture = ROOT / "tests/plugins/slurm_samples/basic"
    output = tmp_path / "results"
    flags = ["-G"] if realtime else []
    result = run_cli(
        tmp_path, "qtop_py.cli", "-b", "slurm", "-s", str(fixture), "-O", "-c", "OFF", "-o", "savepath=" + str(output), "-o", "extract_info=" + repr({key: command}), *flags
    )
    assert result.returncode == 0, result.stderr
    assert "Traceback" not in result.stdout + result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert "User accounts and pool mappings" in frame


@pytest.mark.parametrize("slots", [1, 16])
@pytest.mark.parametrize("array_tasks", [False, True])
def test_sge_repeated_job_numbers_preserve_core_ownership(tmp_path, array_tasks, slots):
    job = (
        '<job_list state="running"><JB_job_number>{number}</JB_job_number><JB_name>job</JB_name>'
        "<JB_owner>{user}</JB_owner><state>r</state><JAT_start_time>now</JAT_start_time><slots>{slots}</slots>{task}</job_list>"
    )
    jobs = "".join(
        job.format(number=number, user=user, slots=slots, task="<tasks>%s</tasks>" % task if array_tasks else "")
        for number, user, task in [(100, "alice", 1), (100, "alice", 2), (200, "bob", 1)]
    )
    trace = (
        f"<job_info><queue_info><Queue-List><name>batch@node01</name><slots_used>{3 * slots}</slots_used><slots_total>{3 * slots}</slots_total>"
        f'<resource name="qname">batch</resource><resource name="hostname">node01</resource><resource name="num_proc">{3 * slots}</resource>'
        + jobs
        + "</Queue-List></queue_info><job_info/></job_info>"
    )
    (tmp_path / "qstat.F.xml.stdout").write_text(trace)
    output = tmp_path / "results"
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "sge", "-s", str(tmp_path), "-O", "-c", "OFF", "-o", "savepath=" + str(output))
    assert result.returncode == 0, result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    rows = dict(re.findall(r"\[\s*\d+\]\s+(\w+)\s*\|[^|]*\|\s*(\d+)\s*\|", frame))
    assert rows == {"alice": "1", "bob": "1"}
    assert "%s/%s cores" % (3 * slots, 3 * slots) in frame
    assert "3+0 jobs (R + Q)" in frame


def test_sge_summary_marks_errors_from_later_queue_instances(tmp_path):
    tree = etree.parse(str(ROOT / "qtop_py/contrib/qstat.F.xml.stdout"))
    queues = tree.getroot().findall("queue_info/Queue-List")
    name = queues[0].find("resource[@name='qname']").text
    for queue in queues:
        state = queue.find("state")
        if state is not None:
            queue.remove(state)
    instances = [queue for queue in queues if queue.find("resource[@name='qname']").text == name]
    assert len(instances) > 1
    etree.SubElement(instances[-1], "state").text = "E"
    tree.write(str(tmp_path / "qstat.F.xml.stdout"))
    output = tmp_path / "results"
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "sge", "-s", str(tmp_path), "-O", "-c", "OFF", "-o", "savepath=" + str(output))
    assert result.returncode == 0, result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert name + "*: " in frame
    assert "Pending*: " not in frame
    assert "* implies blocked (at least one queue instance)" in frame


@pytest.mark.parametrize(
    "rule, cores",
    [
        ({"include_name_patterns": ["^node03$"]}, 1),
        ({"exclude_name_patterns": ["^N/A$"]}, 2),
    ],
)
def test_sge_filters_work_without_blind_remapping(tmp_path, rule, cores):
    template = (
        "<Queue-List><name>batch@node0{number}</name><slots_used>1</slots_used>"
        '<resource name="qname">batch</resource><resource name="hostname">node0{number}</resource>'
        '<resource name="num_proc">1</resource><job_list state="running"><JB_job_number>{number}</JB_job_number>'
        "<JB_name>job</JB_name><JB_owner>alice</JB_owner><state>r</state><JAT_start_time>now</JAT_start_time>"
        "<slots>1</slots></job_list></Queue-List>"
    )
    trace = "<job_info><queue_info>" + "".join(template.format(number=n) for n in [1, 3]) + "</queue_info><job_info/></job_info>"
    (tmp_path / "qstat.F.xml.stdout").write_text(trace)
    output = tmp_path / "results"
    result = run_cli(
        tmp_path,
        "qtop_py.cli",
        "-b",
        "sge",
        "-s",
        str(tmp_path),
        "-O",
        "-F",
        "-c",
        "OFF",
        "-o",
        "savepath=" + str(output),
        "-o",
        "filtering=" + repr([rule]),
    )
    assert result.returncode == 0, result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert "%s/%s cores" % (cores, cores) in frame
    assert "N/A" not in frame
    assert "User accounts and pool mappings" in frame


@pytest.mark.parametrize("details", [False, True])
def test_slurm_cpu_placement_legend_and_resource_export(tmp_path, details):
    import json
    import shutil

    source = tmp_path / "trace"
    shutil.copytree(str(ROOT / "tests/plugins/slurm_samples/cpu_allocations"), str(source))
    if not details:
        (source / "scontrol.txt").unlink()
    output = tmp_path / "results"
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "slurm", "-s", str(source), "-E", "-O", "-c", "OFF", "-o", "savepath=" + str(output))
    assert result.returncode == 0, result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert ("scheduler CPU IDs" if details else "estimated where") in frame
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert state["jobs"][0]["slurm_tres_alloc"] == "cpu=4,gres/gpu=2"
    assert state["nodes"][0]["slurm_gres_used"] == "gpu:a100:1(IDX:1)"
    if details:
        assert state["nodes"][1]["core_job_map"] == {"1": "101", "3": "101", "5": "101"}


@pytest.mark.parametrize("available", [False, True])
def test_default_live_slurm_details_command_failure_keeps_monitoring(tmp_path, available):
    import json
    import shlex

    source = ROOT / "tests/plugins/slurm_samples/cpu_allocations"
    commands = tmp_path / "commands"
    commands.mkdir()
    for command in ("squeue", "sinfo", "scontrol"):
        executable = commands / command
        body = "exit 1" if command == "scontrol" and not available else "cat " + shlex.quote(str(source / (command + ".txt")))
        executable.write_text("#!/bin/sh\n" + body + "\n")
        executable.chmod(0o755)
    output = tmp_path / "results"
    result = run_cli(
        tmp_path,
        "qtop_py.cli",
        "-b",
        "slurm",
        "-E",
        "-O",
        "-c",
        "OFF",
        "-o",
        "savepath=" + str(output),
        extra_env={"PATH": str(commands) + os.pathsep + os.environ.get("PATH", "")},
    )
    assert result.returncode == 0, result.stderr
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert len(state["jobs"]) == 3
    assert state["nodes"][0]["cpu_allocation_source"] == ("slurm" if available else "estimated")


def test_slurm_array_tasks_and_heterogeneous_components_render_consistently(tmp_path):
    import json

    source = ROOT / "tests/plugins/slurm_samples/arrays_and_het"
    output = tmp_path / "results"
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "slurm", "-s", str(source), "-E", "-O", "-c", "OFF", "-o", "savepath=" + str(output))
    assert result.returncode == 0, result.stderr
    frame = next(output.glob("qtop_fullview_*.out")).read_text()
    assert "4+4 jobs" in frame
    assert "7/16 cores" in frame
    assert "individual array tasks and heterogeneous components" in frame
    rows = {row[0]: tuple(map(int, row[1:])) for row in re.findall(r"\[\s*\d+\]\s+(\w+)\s*\|\s*(\d+)\s+(\d+)\s+(\d+)\s*\|\s*(\d+)\s*\|", frame)}
    assert rows["alice"] == (6, 2, 4, 2)
    assert rows["bob"] == (2, 2, 0, 2)
    state = json.loads(next(output.glob("qtop_json_*.json")).read_text())
    assert len(state["jobs"]) == 8
    assert state["jobs"][0]["slurm_array_job_id"] == "101"
    assert state["jobs"][0]["slurm_array_task_id"] == "1"
    assert state["jobs"][-1]["slurm_het_job_id"] == "201"
    assert state["jobs"][-1]["slurm_het_job_offset"] == "1"
    assert state["nodes"][1]["core_job_map"]["7"] == "201+1"


def test_truncated_slurm_array_trace_fails_with_collection_advice(tmp_path):
    (tmp_path / "squeue.txt").write_text("101_[1-...]|alice|PD|compute|1|(Priority)\n")
    (tmp_path / "sinfo.txt").write_text("node001|compute|idle|8\n")
    result = run_cli(tmp_path, "qtop_py.cli", "-b", "slurm", "-s", str(tmp_path), "-O", "-o", "savepath=" + str(tmp_path / "results"))
    assert result.returncode == 1
    assert "collect squeue with -r/--array" in result.stderr
    assert "Traceback" not in result.stderr
