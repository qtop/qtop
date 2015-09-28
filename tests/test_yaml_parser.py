__author__ = 'sfranky'

import pytest
from yaml_parser import *


@pytest.mark.parametrize('fin, t',
        (
            (
                ['scheduler: sge'],
                [0, "scheduler:", "sge"]
            ),
            (
                ['schedulers:'],
                [0, "schedulers:"]
            ),
            (
                ['  pbs:'],
                [1, "pbs:"]
            ),
            (
                ["   - r'moonshot'"],
                [1, "-", "r'moonshot'"]
            ),
            (
                [" - '\w*cms048': Blue"],
                [1, "-", "'\\w*cms048': Blue"]
            ),
            (
                ["term_size: [53, 176]"],
                [0, "term_size:", "[53, 176]"]
            ),
        ),
)
def test_get_line(fin, t):
    get_lines = get_line(fin, verbatim=False)
    actual_t = next(get_lines)
    assert actual_t == t


@pytest.mark.parametrize('fin, t',
        (
            (
                [
                    '  oar:\n',
                    '    oarnodes_s_file: oarnodes_s_Y.txt\n'
                ],
                [
                    [1, "oar:"],
                    [1, 'oarnodes_s_file:', 'oarnodes_s_Y.txt']
                ]
            ),
            (
                [
                    '    oarstat_file: oarstat.txt\n',
                    '  sge:'
                ],
                [
                    [1, "oarstat_file:", "oarstat.txt"],
                    [-1, "sge:"]
                ]
            ),
            (
                [
                    '    sge_file_stat: qstat.F.xml.stdout',
                    '\n',
                    'faster_xml_parsing: False'
                ],
                [
                    [1, 'sge_file_stat:', 'qstat.F.xml.stdout'],
                    [-1],
                    [0, 'faster_xml_parsing:', 'False'],
                ]
            )
        ),
)
def test_get_more_lines(fin, t):
    get_lines = get_line(fin, verbatim=False)
    for idx, line in enumerate(fin):
        actual_t = next(get_lines)
        assert actual_t == t[idx]
