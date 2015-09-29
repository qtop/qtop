__author__ = 'sfranky'

import pytest
from yaml_parser import *

# @pytest.mark.skipif(True, reason="No special reason")
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

# @pytest.mark.skipif(True, reason="No special reason")
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
    get_lines = get_line(fin)
    for idx, line in enumerate(fin):
        actual_t = next(get_lines)
        assert actual_t == t[idx]


# @pytest.mark.skipif(True, reason="No special reason")
@pytest.mark.parametrize('fin, line, get_lines, key_container, last_empty_container',  # get_line(fin)
     (
         (
             ['testkey: testvalue'], [0, 'testkey:', 'testvalue'], get_line(['testkey: testvalue']), {'testkey:': 'testvalue'}, {}
         ),
         (
             ['testkey:'], [0, 'testkey:'], get_line(['testkey:']), {'testkey:': {}}, {}
         ),
         (
             ['testkey: [testvalue]'], [0, 'testkey:', '[testvalue]'], get_line(['testkey: [testvalue]']), {'testkey:': '[testvalue]'}, {}
         ),
         (
             ['testkey: |'], [0, 'testkey:', '|'], get_line(['testkey: |']), {'testkey:': '|'}, {}
         ),
         (
             ['- testkey:'], [0, '-', 'testkey:'], get_line(['- testkey:']), {'-': [{'testkey:': {}}]}, {}
         ),
         (
             ['- testkey: testvalue'], [0, '-', 'testkey: testvalue'], get_line(['- testkey: testvalue']), {'-': [{'testkey:': 'testvalue'}]}, {}
         ),
         (
                 ['- testkey: [testvalue]'], [0, '-', 'testkey: [testvalue]'], get_line(['- testkey: [testvalue]']), {'-': [{'testkey:': '[testvalue]'}]}, {}
         ),
         (
             ['- testvalue'], [0, '-', 'testvalue'], get_line(['- testvalue']), {'-': 'testvalue'}, 'testvalue'
         ),
     )
)
def test_process_line(line, fin, get_lines, key_container, last_empty_container):  # parent_container, container_stack, stack):
    assert process_line(line, fin, get_lines, last_empty_container) == (key_container, last_empty_container)

@pytest.mark.current
@pytest.mark.parametrize('line_in, fin, block_in, block_out, line_out',
     (
         (
             [0],
             # """testkey1: testvalue1\ntestkey2: testvalue2\ntestkey3: testvalue3\n""".split('\n'),
"""
testkey1: testvalue1
testkey2: testvalue2
testkey3: testvalue3
""".split('\n'),
             {},
             {'testkey1:': 'testvalue1', 'testkey2:': 'testvalue2', 'testkey3:': 'testvalue3'},
             [0],
         ),
         (
             [0],
             # """testkey1: testvalue1\ntestkey2: testvalue2\ntestkey3: testvalue3\n""".split('\n'),
"""
testkey1:
  testkey2:
    testkey3: value3
    testkey4: value4
""".split('\n'),
             {},
             {'testkey1:': {'testkey2:': {'testkey3:': 'value3', 'testkey4:': 'value4'}}},
             [-1],
         ),
         (
             [0],
"""
testkey1:
  - testkey2:
     testkey3: value3
  - testkey4:
     testkey5: value5
  - testkey6: value6
  - value7
""".split('\n'),
             {},
             {'testkey1:':
                  {
                    '-':
                       [
                        {'testkey2:': {'testkey3:': 'value3'}},
                        {'testkey4:': {'testkey5:': 'value5'}},
                        {'testkey6:': 'value6'},
                        'value7'
                       ]
                  }
             },
             [-1],
         ),
         (
             [0],
"""
testkey0:
    - testkey:
        testkey1: testvalue1
        testkey2: [testvalue2]
""".split('\n'),
             {},
             {'testkey0:': {'-': [{'testkey:': {'testkey1:': 'testvalue1', 'testkey2:': '[testvalue2]'}}]}},
             [-1],
         ),
         (
             [0],
"""
testkey1:
 - testkey2:
     testkey3: testvalue3
""".split('\n'),
             {},
             {'testkey1:': {'-': [{'testkey2:': {'testkey3:': 'testvalue3'}}]}},
             [-1],
         ),
     )
)
def test_read_yaml_config_block(line_in, line_out, fin, block_in, block_out):
    get_lines = get_line(fin)
    assert read_yaml_config_block(line_in, fin, get_lines, block_in) == (block_out, line_out)
























