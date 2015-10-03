__author__ = 'sfranky'

import os
from itertools import takewhile, dropwhile



def get_line(fin, verbatim=False, SEPARATOR=None):
    """
    Yields a list per line read, of the following form:
    [indentation_change, line up to first space, line after first space if exists]
    Comment lines are omitted.
    Lines where comments exist at the end are stripped off their comment.
    Indentation is calculated with respect to the previous line.
    1: line is further indented
    0: same indentation
    -1: line is unindent
    Empty lines only return the indentation change.
    Where the line splits depends on SEPARATOR (default is first space)
    """
    indent = 0
    for line in fin:
        if line.lstrip().startswith('#'):
            continue
        elif ' #' in line :
            line = line.split(' #', 1)[0]

        prev_indent = indent
        indent = len(line) - len(line.lstrip(' '))
        if indent - prev_indent > 0:
            d_indent = 1
        elif indent == prev_indent:
            d_indent = 0
        else:
            d_indent = -1
        line = line.rstrip()
        list_line = verbatim and [d_indent, line] or [d_indent] + line.split(None or SEPARATOR, 1)
        if len(list_line) > 1:
            if list_line[1].startswith('"') or list_line[1].startswith("'"):
                list_line[1] = list_line[1][1:-1]
        else:
            pass
        yield list_line


def read_yaml_config(_fin):
    raw_key_values = {}
    with open(_fin, mode='r') as fin:
        get_lines = get_line(fin)
        line = next(get_lines)
        while line:
            block = dict()
            block, line = read_yaml_config_block(line, fin, get_lines, block)
            for key in block:
                print key
                print '\t' + str(block[key])
            raw_key_values.update(block)

        config_dict = dict([(key, value) for key, value in raw_key_values.items()])
        return config_dict


def read_yaml_config_block(line, fin, get_lines, block):
    parent_container = block
    open_containers = list()
    open_containers.append(block)

    if len(line) > 1:  # non-empty line
        key_value, parent_container = process_line(line, fin, get_lines, parent_container)
        for (k, v) in key_value.items():
            block[k] = v

    while len(line) == 1:  # skip empty lines
        try:
            line = next(get_lines)
        except StopIteration:  # EO(config)F
            return {}, ''

    while len(line) > 1:  # as long as a blank line is not reached (i.e. block is not complete)
        if line[0] == 0:  # same level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            for k in key_value:
                pass  # assign dict's sole key to k
            if parent_container == {} or '-' not in parent_container:
                parent_container[k] = key_value[k]
            else:
                parent_container.setdefault(k, []).extend(key_value[k])  # was waiting for a list, but a str came in!
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)

        elif line[0] > 0:  # go down one level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            for k in key_value:
                pass
            # if container == {}:  # up parent container with new value
            if parent_container == {}:
                parent_container[k] = key_value[k]
            else:
                parent_container.setdefault(k, []).extend(key_value[k])
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)

        elif line[0] < 0:  # go up one level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            open_containers.pop()
            for k in key_value:
                pass
            if open_containers[-1].get('-'):
                open_containers[-1].setdefault('-', []).extend(key_value[k])
            else:
                open_containers[-1][k] = key_value[k]
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)
            else:
                parent_container = open_containers[-1]
            # root_key = next(open_containers[0].iterkeys())
            # block[root_key].update(key_value)
        try:
            line = next(get_lines)
        except StopIteration:
            return block, ''

    return block, line


def process_line(list_line, fin, get_lines, parent_container):
    key = list_line[1]

    if len(list_line) == 2:  # key-only, so what's in the line following should be written in a new container
        container = {}
        return {key.rstrip(':'): container}, container

    elif len(list_line) == 3:
        container = list_line[2]

        if container.endswith(':'):  # key: '-'           - testkey:
            parent_key = key
            key = container
            new_container = {}
            return {parent_key: [{key.rstrip(':'): new_container}]}, new_container  #list

        elif ': ' in container:  # key: '-'               - testkey: testvalue
            parent_key = key
            key, container = container.split(None, 1)
            container = [container[1:-1]] if container.startswith('[') else container
            return {'-': [{key.rstrip(':'): container}]}, container  #list

        elif container.endswith('|'):
            container = process_code(fin)
            return {key.rstrip(':'): container}, parent_container

        else:  # simple value
            if key == '-':  # i.e.  - testvalue
                return {'-': [container]}, container  # was parent_container******was :[container]}, container
            else:  # i.e. testkey: testvalue
                container = [container[1:-1]] if container.startswith('[') else container  #list
                return {key.rstrip(':'): container}, container  # was parent_container#str
    else:
        raise ValueError("Didn't anticipate that!")


def process_code(fin):
    get_code = get_line(fin, verbatim=True)
    # line = next(get_code)
    line = next(get_code)
    code = []
    while line[0] != -1:
        code.append(' ' + line[-1])
        try:
            line = next(get_code)
        except StopIteration:
            break
    return '\n'.join([c.strip() for c in code]).strip()


def safe_load(fin):
    config_dict = read_yaml_config(fin)
    fin.close()
    return config_dict


if __name__ == '__main__':
    conf_file = '/home/sfranky/.local/qtop/qtopconf.yaml'
    # conf_file = '/home/liveuser/PycharmProjects/qtop/qtopconf.yaml'
    config = read_yaml_config(conf_file)