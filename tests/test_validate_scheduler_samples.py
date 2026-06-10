from tools.validate_scheduler_samples import ROOT, discover_cases


def test_discover_cases_covers_all_shared_validation_backends():
    cases = discover_cases(["pbs", "sge", "slurm", "oar", "demo"], ROOT / "tests/plugins/slurm_samples")

    schedulers = {case["scheduler"] for case in cases}
    names = {case["name"] for case in cases}

    assert schedulers == {"pbs", "sge", "slurm", "oar", "demo"}
    assert {"pbs-contrib", "sge-contrib", "oar-contrib", "demo-generated"} <= names
    assert any(name.startswith("slurm-") for name in names)


def test_shared_validation_cases_request_coloured_output():
    cases = discover_cases(["pbs", "sge", "slurm", "oar", "demo"], ROOT / "tests/plugins/slurm_samples")

    for case in cases:
        args = case.get("args", ["-c", "ON"])
        assert args[args.index("-c") + 1] == "ON"
