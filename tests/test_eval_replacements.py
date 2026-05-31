from types import SimpleNamespace

from qtop_py import qtop


def test_extract_detail_from_field_supports_configured_regex():
    detail = qtop.extract_detail_from_field(
        "alice:x:1000:1000:Alice Example <alice@example.org>:/home/alice:/bin/bash",
        "re.search('(?<=<)[^<>]+(?=>)', field).group(0)",
    )

    assert detail == "alice@example.org"


def test_update_config_with_cmdline_vars_uses_literals_without_eval():
    args = SimpleNamespace(OPTION=["transpose_wn_matrices=True", "sample_limit=5", "label=prod"], TRANSPOSE=False, REM_EMPTY_CORELINES=0)
    config = {"rem_empty_corelines": "1"}

    updated = qtop.update_config_with_cmdline_vars(args, config)

    assert updated["transpose_wn_matrices"] is True
    assert updated["sample_limit"] == 5
    assert updated["label"] == "prod"


def test_do_name_remapping_supports_documented_offset_lambda_without_eval():
    cluster = qtop.Cluster.__new__(qtop.Cluster)
    cluster.config = {
        "workernodes_matrix": [{"wn id lines": {"max_len": 0}}],
        "remapping": [{"([A-Za-z]+)([0-9]+)$": "lambda m: m.group(1)+str(int(m.group(2))+250)"}],
    }

    remapped = cluster.do_name_remapping({1: {"domainname": "wn100.example.org"}})

    assert remapped[1]["host"] == "wn350"


def test_sort_worker_nodes_uses_registered_sort_keys_without_eval():
    previous_dynamic = getattr(qtop, "dynamic_config", None)
    qtop.dynamic_config = {}
    try:
        cluster = qtop.Cluster.__new__(qtop.Cluster)
        cluster.config = {"sorting": {"user_sort": ["sort by all numbers"], "reverse": False}}
        cluster.worker_nodes = [
            {"domainname": "node10", "state": "-", "np": "1", "core_job_map": {}},
            {"domainname": "node2", "state": "-", "np": "1", "core_job_map": {}},
        ]

        sorted_nodes = cluster._sort_worker_nodes()

        assert [node["domainname"] for node in sorted_nodes] == ["node2", "node10"]
    finally:
        if previous_dynamic is None:
            del qtop.dynamic_config
        else:
            qtop.dynamic_config = previous_dynamic
