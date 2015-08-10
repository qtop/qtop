__author__ = 'sfranky'

import re
import pytest


@pytest.mark.parametrize('domain_name, match',
        (
         ('lrms123', 'lrms123'),
         ('td123.pic.es', 'td123'),
         ('gridmon.ucc.ie', 'gridmon'),
         ('gridmon.cs.tcd.ie', 'gridmon'),
         ('wn123.grid.ucc.ie', 'wn123'),
         ('lcg123.gridpp.rl.ac.uk', 'lcg123'),
         ('compute-123-123', 'compute-123-123'),
         ('wn-hp-123.egi.local', 'wn-hp-123'),
         ('woinic-123.egi.local', 'woinic-123'),
         ('wn123-ara.bifi.unizar.es', 'wn123-ara'),
         ('c123-123-123.gridka.de', 'c123-123-123'),
         ('n123-iep-grid.saske.sk', 'n123-iep-grid'),
        ),
    )
def test_re_node(domain_name, match):
    re_node = '([A-Za-z0-9-]+)(?=\.|$)'
    m = re.search(re_node, domain_name)
    try:
        assert m.group(0) == match
    except AttributeError:
        assert False

