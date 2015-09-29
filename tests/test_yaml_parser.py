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


# @pytest.mark.parametrize('line, fin, get_lines, parent_container, container_stack, stack',
# @pytest.mark.parametrize('fin, line, get_lines, key_container',  # get_line(fin)
#      (
#          (
#              ['testkey: testvalue'], [0, 'testkey:', 'testvalue'], get_line(['testkey: testvalue']), {'testkey:': 'testvalue'}
#          ),
#          (
#              ['testkey:'], [0, 'testkey:'], get_line(['testkey:']), {'testkey:': {}}
#          ),
#          (
#              ['testkey: [testvalue]'], [0, 'testkey:', '[testvalue]'], get_line(['testkey: [testvalue]']), {'testkey:': '[testvalue]'}
#          ),
#          (
#              ['testkey: |'], [0, 'testkey:', '|'], get_line(['testkey: |']), {'testkey:': '|'}
#          ),
#          (
#              ['- testkey:'], [0, '-', 'testkey:'], get_line(['- testkey:']), {'-': 'testkey:'}
#          ),
#          (
#              ['- testkey: testvalue'], [0, '-', 'testkey: testvalue'], get_line(['- testkey: testvalue']), {'-': 'testkey: testvalue'}
#          ),
#          (
#              ['- testkey: [testvalue]'], [0, '-', 'testkey: [testvalue]'], get_line(['- testkey: [testvalue]']), {'-': 'testkey: [testvalue]'}
#          ),
#          (
#              ['- testvalue'], [0, '-', 'testvalue'], get_line(['- testvalue']), {'-': 'testvalue'}
#          ),
#      )
# )
# def test_process_line(line, fin, get_lines, key_container):  # parent_container, container_stack, stack):
#     assert key_container == process_line(line, fin, get_lines)

# @pytest.mark.skipif(True, reason="No special reason")
@pytest.mark.parametrize('fin, line, get_lines, key_container',  # get_line(fin)
     (
         (
             ['testkey: testvalue'], [0, 'testkey:', 'testvalue'], get_line(['testkey: testvalue']), {'testkey:': 'testvalue'}
         ),
         (
             ['testkey:'], [0, 'testkey:'], get_line(['testkey:']), {'testkey:': {}}
         ),
         (
             ['testkey: [testvalue]'], [0, 'testkey:', '[testvalue]'], get_line(['testkey: [testvalue]']), {'testkey:': '[testvalue]'}
         ),
         (
             ['testkey: |'], [0, 'testkey:', '|'], get_line(['testkey: |']), {'testkey:': '|'}
         ),
         (
             ['- testkey:'], [0, '-', 'testkey:'], get_line(['- testkey:']), {'-': {'testkey:': {}}}
         ),
         (
             ['- testkey: testvalue'], [0, '-', 'testkey: testvalue'], get_line(['- testkey: testvalue']), {'-': {'testkey:': 'testvalue'}}
         ),
         (
             ['- testkey: [testvalue]'], [0, '-', 'testkey: [testvalue]'], get_line(['- testkey: [testvalue]']), {'-': {'testkey:': '[testvalue]'}}
         ),
         (
             ['- testvalue'], [0, '-', 'testvalue'], get_line(['- testvalue']), {'-': 'testvalue'}
         ),
     )
)
def test_process_line(line, fin, get_lines, key_container):  # parent_container, container_stack, stack):
    assert key_container == process_line(line, fin, get_lines)


@pytest.mark.parametrize('line_in, fin, get_lines, block_in, block_out, line_out',
     (
         (
             [0, 'testkey1:', 'testvalue1'],
             """testkey1: testvalue1
             testkey2: testvalue2
             testkey3: testvalue3

             """,
             None,
             {},
             {'testkey1:': 'testvalue1', 'testkey2:': 'testvalue2', 'testkey3:': 'testvalue3'},
             [0],
         ),
     )
)
def test_read_yaml_config_block(line_in, line_out, fin, get_lines, block_in, block_out):
    assert read_yaml_config_block(line_in, fin, get_lines, block_in) == (block_out, line_out)
