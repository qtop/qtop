from types import SimpleNamespace

from qtop_py.qtop import Cluster, extract_detail_from_field, parse_config_literal, update_config_with_cmdline_vars
from qtop_py.yaml_parser import read_yaml_config_block, get_line


def test_parse_config_literal_does_not_execute_code():
    value = "__import__('os').system('echo unsafe')"
    assert parse_config_literal(value) == value


def test_cmdline_override_parses_literals_without_eval():
    args = SimpleNamespace(OPTION=["enabled=True", "name=plain-text"], TRANSPOSE=False, REM_EMPTY_CORELINES=0)
    config = {"rem_empty_corelines": "0"}
    assert update_config_with_cmdline_vars(args, config) == {
        "enabled": True,
        "name": "plain-text",
        "rem_empty_corelines": 0,
    }


def test_legacy_gecos_regex_is_handled_without_eval():
    regex = "re.search('(?<=<)[^<>]+(?=>)', field).group(0)"
    assert extract_detail_from_field("ignored:<CN=Queue User>:ignored", regex) == "CN=Queue User"


def test_yaml_parser_keeps_non_literal_list_values_as_text():
    fin = ["testkey: [__import__('os').system('echo unsafe')]"]
    get_lines = get_line(fin)
    block, _ = read_yaml_config_block(next(get_lines), fin, get_lines)
    assert block == {"testkey": ["__import__('os').system('echo unsafe')"]}


def test_cluster_sort_uses_named_rules_without_eval(monkeypatch):
    import qtop_py.qtop as qtop_module

    monkeypatch.setattr(qtop_module, "dynamic_config", {}, raising=False)
    cluster = Cluster.__new__(Cluster)
    cluster.config = {"sorting": {"user_sort": ["sort by nodename-notnum", "sort by all numbers"], "reverse": False}}
    cluster.worker_nodes = [
        {"domainname": "node10", "state": "R", "np": "8", "core_job_map": {"0": "job"}},
        {"domainname": "node2", "state": "R", "np": "8", "core_job_map": {}},
    ]

    sorted_nodes = cluster._sort_worker_nodes()
    assert [node["domainname"] for node in sorted_nodes] == ["node2", "node10"]
