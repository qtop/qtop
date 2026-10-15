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
