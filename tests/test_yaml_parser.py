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
            ['schedulers'],
            [0, "schedulers"]
        ),
        (
            ['  pbs'],
            [1, "pbs"]
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
                '  sge'
            ],
            [
                [1, "oarstat_file:", "oarstat.txt"],
                [-1, "sge"]
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
             ['testkey: testvalue'], [0, 'testkey', 'testvalue'], get_line(['testkey: testvalue']), {'testkey': 'testvalue'}, {}
         ),
         (
             ['testkey'], [0, 'testkey'], get_line(['testkey']), {'testkey': {}}, {}
         ),
         (
             ['testkey: [testvalue]'], [0, 'testkey', '[testvalue]'], get_line(['testkey: [testvalue]']), {'testkey': '[testvalue]'}, {}
         ),
         (
             ['testkey: |'], [0, 'testkey', '|'], get_line(['testkey: |']), {'testkey': '|'}, {}
         ),
         (
             ['- testkey'], [0, '-', 'testkey'], get_line(['- testkey']), {'-': ['testkey']}, 'testkey'
         ),
         (
             ['- testkey: testvalue'], [0, '-', 'testkey: testvalue'], get_line(['- testkey: testvalue']), {'-': [{'testkey': 'testvalue'}]}, {}
         ),
         (
             ['- testkey: [testvalue]'], [0, '-', 'testkey: [testvalue]'], get_line(['- testkey: [testvalue]']), {'-': [{'testkey': '[testvalue]'}]}, {}
         ),
         (
             ['- testvalue'], [0, '-', 'testvalue'], get_line(['- testvalue']), {'-': ['testvalue']}, 'testvalue'
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
             {'testkey1': 'testvalue1', 'testkey2': 'testvalue2', 'testkey3': 'testvalue3'},
             [0],
         ),
         (
             [0],
             # """testkey1: testvalue1\ntestkey2: testvalue2\ntestkey3: testvalue3\n""".split('\n'),
"""
testkey4:
  testkey5:
    testkey6: value6
    testkey7: value7
""".split('\n'),
             {},
             {'testkey4': {'testkey5': {'testkey6': 'value6', 'testkey7': 'value7'}}},
             [-1],
         ),
         (
             [0],
"""
testkey8:
  - testkey9:
     testkey10: value10
  - testkey11:
     testkey12: value12
  - testkey13: value13
  - value14
""".split('\n'),
             {},
             {'testkey8':
                  {
                    '-':
                       [
                        {'testkey9': {'testkey10': 'value10'}},
                        {'testkey11': {'testkey12': 'value12'}},
                        {'testkey13': 'value13'},
                        'value14'
                       ]
                  }
             },
             [-1],
         ),
         (
             [0],
"""
testkey15:
    - testkey16:
        testkey17: testvalue17
        testkey18: [testvalue18]
""".split('\n'),
             {},
             {'testkey15': {'-': [{'testkey16': {'testkey17': 'testvalue17', 'testkey18': '[testvalue18]'}}]}},
             [-1],
         ),
         (
             [0],
"""
testkey19:
 - testkey20:
     testkey21: testvalue21
""".split('\n'),
             {},
             {'testkey19': {'-': [{'testkey20': {'testkey21': 'testvalue21'}}]}},
             [-1],
         ),
         (
             [0],
"""
testkey22:  # order should be from more generic-->more specific
 - testkey23: testvalue23
 - testkey24: testvalue24
 - testkey25: testvalue25
""".split('\n'),
             {},
             {'testkey22': {'-': [{'testkey23': 'testvalue23'}, {'testkey24': 'testvalue24'}, {'testkey25': 'testvalue25'}]}},
             [-1],
         ),
         (
             [0],
"""
testkey26:
 - testvalue27
 - testvalue28
 - testvalue29
""".split('\n'),
             {},
             {'testkey26': {'-': ['testvalue27', 'testvalue28', 'testvalue29']}},
             [-1],
         ),
         (
             [0],
"""
state_abbreviations:
  pbs:
    Q: queued_of_user
    E: exiting_of_user
    W: waiting_of_user
  oar:
    E: Error
    F: Finishing
    S: cancelled_of_user
  sge:
    r: running_of_user
    E: exiting_of_user  # not real
    qw: queued_of_user
""".split('\n'),
             {},
             {'state_abbreviations': {'pbs': {'Q': 'queued_of_user', 'E': 'exiting_of_user', 'W': 'waiting_of_user'},
                                       'oar': {'E': 'Error', 'F': 'Finishing', 'S': 'cancelled_of_user'}, 'sge': {'r': 'running_of_user', 'E': 'exiting_of_user', 'qw': 'queued_of_user'}}},
             [-1],
         ),
     ), ids=[
        "3dicts",
        "dict2items_inside_doubly_nested_dict",
        "listof_3dicts_and_a_value_inside_dict",
        "1",
        "2",
        "3",
        "lov_in_dict",
        "state_abbreviations"
    ]
)
def test_read_yaml_config_block(line_in, line_out, fin, block_in, block_out):
    get_lines = get_line(fin)
    assert read_yaml_config_block(line_in, fin, get_lines, block_in) == (block_out, line_out)






































