##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

from pathlib import Path
from xml.etree import ElementTree as etree

import pytest

from qtop_py.plugins.sge import SGEBatchSystem, SGEStatExtractor


class Options(object):
    ANONYMIZE = False


def test_contrib_queue_counts_remain_numeric_while_aggregating():
    sample = Path(__file__).resolve().parents[2] / "qtop_py" / "contrib" / "qstat.F.xml.stdout"
    sge = SGEBatchSystem({"sge_file": str(sample)}, {}, Options())

    sge.get_jobs_info()
    total_running, total_queued, queues = sge.get_queues_info()

    assert isinstance(total_running, int)
    assert isinstance(total_queued, int)
    assert total_running == sum(int(queue["run"]) for queue in queues)
    assert next(queue for queue in queues if queue["queue_name"] == "Pending")["state"] == "E"


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-16-be"])
def test_scheduler_xml_accepts_predefined_entities(tmp_path, encoding):
    sample = tmp_path / "qstat.xml"
    sample.write_bytes("<job_info><name>alice &amp; bob</name></job_info>".encode(encoding))

    extractor = SGEStatExtractor({}, Options(), {})
    tree, root = extractor.get_xml_tree(str(sample))

    assert tree.getroot() is root
    assert root.find("name").text == "alice & bob"


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-16-be"])
@pytest.mark.parametrize(
    "xml",
    [
        '<!DOCTYPE job_info [<!ENTITY value "expanded">]><job_info>&value;</job_info>',
        '<!DOCTYPE job_info [<!ENTITY a "expanded"><!ENTITY b "&a;&a;">]><job_info>&b;</job_info>',
        '<!DOCTYPE job_info SYSTEM "file:///nonexistent"><job_info/>',
        '<!DOCTYPE job_info [<!ENTITY value SYSTEM "file:///nonexistent">]><job_info>&value;</job_info>',
        "<!DOCTYPE job_info><job_info/>",
    ],
    ids=["internal-entity", "nested-entities", "external-dtd", "external-entity", "empty-dtd"],
)
def test_scheduler_xml_rejects_dtds(tmp_path, encoding, xml):
    sample = tmp_path / "qstat.xml"
    sample.write_bytes(xml.encode(encoding))
    extractor = SGEStatExtractor({}, Options(), {})

    with pytest.raises(etree.ParseError, match="DTD declarations are not supported in scheduler XML"):
        extractor.get_xml_tree(str(sample))


def test_scheduler_xml_still_reports_malformed_input(tmp_path):
    sample = tmp_path / "qstat.xml"
    sample.write_bytes(b"<job_info>")
    extractor = SGEStatExtractor({}, Options(), {})

    with pytest.raises(etree.ParseError):
        extractor.get_xml_tree(str(sample))


@pytest.mark.parametrize(
    "running, field",
    [(False, field) for field in ("JB_owner", "JB_job_number", "JB_name", "state")]
    + [(True, field) for field in ("JB_owner", "JB_job_number", "JB_name", "state", "JAT_start_time")],
)
@pytest.mark.parametrize("empty", (False, True))
def test_job_xml_reports_missing_or_empty_required_fields(field, empty, running):
    queue = etree.fromstring(
        '<Queue-List><job_list state="running"><JB_owner>alice</JB_owner>'
        "<JB_job_number>123</JB_job_number><JB_name>job</JB_name>"
        "<state>r</state><JAT_start_time>now</JAT_start_time></job_list></Queue-List>"
    )
    job = queue.find("job_list")
    element = job.find(field)
    if empty:
        element.text = None
    else:
        job.remove(element)
    system = SGEBatchSystem({}, {}, Options())
    with pytest.raises(etree.ParseError, match=field):
        if running:
            system._extract_running_job_info(queue, "job_list")
        else:
            system.sge_stat_maker._extract_job_info([], queue, "job_list", "batch")


def test_optional_queue_xml_fields_keep_defaults():
    queue = etree.fromstring('<Queue-List><resource name="qname">batch</resource></Queue-List>')
    root = etree.Element("root")
    root.append(queue)
    system = SGEBatchSystem({}, {}, Options())
    assert system._get_state(queue) == "-"
    assert system._get_host_qname_np(queue)["qname"] == set()
    assert system._extract_queues("Queue-List", root)[0]["state"] == "?"


@pytest.mark.parametrize("anonymize", (False, True))
def test_empty_optional_submission_time_is_ignored(anonymize):
    queue = etree.fromstring(
        "<Queue-List><job_list><JB_owner>alice</JB_owner><JB_job_number>123</JB_job_number><JB_name>job</JB_name><state>qw</state><JB_submission_time/></job_list></Queue-List>"
    )
    options = Options()
    options.ANONYMIZE = anonymize
    extractor = SGEStatExtractor({}, options, {})
    records = extractor._extract_job_info([], queue, "job_list", "batch")
    assert len(records) == 1
    assert records[0]["S"] == "qw"


@pytest.mark.parametrize(
    "tasks,expected",
    [("", "100"), ("<tasks/>", "100"), ("<tasks> </tasks>", "100"), ("<tasks>1</tasks>", "100.1"), ("<tasks>2</tasks>", "100.2"), ("<tasks>1-100:1</tasks>", "100.1-100:1")],
)
def test_job_and_core_records_share_task_identity(tasks, expected):
    queue = etree.fromstring(
        '<Queue-List><job_list state="running"><JB_owner>alice</JB_owner>'
        "<JB_job_number>100</JB_job_number><JB_name>job</JB_name><state>r</state>"
        "<JAT_start_time>now</JAT_start_time><slots>1</slots>" + tasks + "</job_list></Queue-List>"
    )
    system = SGEBatchSystem({}, {}, Options())
    records = system.sge_stat_maker._extract_job_info([], queue, "job_list", "batch")
    assert records[0]["JobId"] == expected
    assert system._extract_running_job_info(queue, "job_list")[0] == [expected]
