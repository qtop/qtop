__author__ = 'sfranky'

import os
from itertools import takewhile, dropwhile
conf_file = '/home/sfranky/.local/qtop/qtopconf.yaml'


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
        yield verbatim and [d_indent, line] or [d_indent] + line.split(None or SEPARATOR, 1)


def read_yaml_config(fn):
    raw_key_values = {}
    with open(fn, mode='r') as fin:
        get_lines = get_line(fin)
        line = next(get_lines)
        while line:
            block = dict()
            block, line = _read_yaml_config_block(line, fin, get_lines, block)
            # for key in block:
            #     print key
            #     print '\t' + str(block[key])
            raw_key_values.update(block)

        config_dict = dict([(key, value) for key, value in raw_key_values.items()])
        return config_dict


def _read_yaml_config_block(line, fin, get_lines, block):
    if len(line) > 1:  # non-empty line
        key, container = process_key_value_line(line, fin, get_lines)
        block[key] = container

    while len(line) == 1:
        line = next(get_lines)
    while len(line) > 1:  # as long as a blank line is not reached (i.e. block is not complete)
        if line[0] > 0:  # nesting
            container = process_line(line, fin, get_lines, parent_container, container_stack, stack)
        elif line[0] == 0:  # same level
            container = process_line(line, fin, get_lines, parent_container, container_stack, stack)
        elif line[0] < 0:  # go up one level
            parent_container = container_stack[-1]
            key, container = process_key_value_line(line, fin, get_lines)
            parent_container[key] = container
        line = next(get_lines)

    return block, line


# def process_line(line, fin, get_lines, parent_container, container_stack, stack):
#     container_stack.append(parent_container)
#     if line[1].endswith(':'):
#         key, container = process_key_value_line(line, fin, get_lines)
#         parent_container[key] = container
#     elif line[1] == '-':
#         container_stack.append(parent_container)
#         container, stack = process_list_item_line(line, fin, stack, parent_container)
#         # (container is {list_out_by_name_pattern: dict()})
#         parent_container.setdefault('-',[]).append(container)
#     return container  # want to return a {} here for by_name_pattern


def process_line(line, fin, get_lines):
    key = line[1]
    if len(line) == 2:
        container = {}
    elif len(line) == 3:
        container = line[2]
        if ': ' in container:
            parent_key = key
            # parent_container = container
            key, container = container.split(None, 1)
            return {parent_key: {key: container}}
        elif container.endswith(':'):
            parent_key = key
            key = container
            container = {}
            return {parent_key: {key: container}}
    else:
        raise ValueError("Didn't anticipate that!")
    return {key: container}


def process_container(line, container, fin):
    if len(line) > 2:
        get_nested_container = get_line([container])  # SEPARATOR=': '
        nested_container = next(get_nested_container)[1:]
        key, container = process_line(line, fin, get_nested_container)


def process_key_value_line(line, fin, get_lines=None):
    key = line[1].rstrip(':')
    if len(line) == 3:  # key-value in same line
        value = line[2]
        if value == '|':
            value = process_code(fin)
    else:
        value = {}
        # value = None
    value = process_value(value, fin)
    return key, value


def process_list_item_line(line, fin, stack, parent_container):
    stack +=1
    value = line[-1]
    container = process_value(value, fin)
    return container, stack


def process_code(fin):
    # line = next(get_lines)
    # code = line[1]
    # while line[0] != -1:
    #     code += ' ' + line[-1]
    #     line = next(get_lines)
    # return code
    get_code = get_line(fin, verbatim=True)
    line = next(get_code)
    code = []
    while line[0] != -1:
        code.append(' ' + line[-1])
        line = next(get_code)
    return ' '.join(code.strip())


def process_value(_value, fin):
    if _value == 'False':
        return eval(_value)
    elif _value in [{}, None]:
        return _value
    elif isinstance(_value, list):
        value = _value[0].rstrip()
        if value.endswith(':'):
            key = value.rstrip(':')
            value = {}
        elif ':' in value:
            key, value = value.split(None, 1)
        else:
            key = '-'
        new_line = [0, key, value]
        key, value = process_key_value_line(new_line, fin, None)
        d = {key: value}
        return [d]
    elif ': ' in _value:
        key, _value = _value.split(': ')
        value = eval(repr(_value))
        d = {key: value}
    elif _value.endswith(':'):
        key = _value.rstrip(':')
        value = dict()
        d = {key: value}
    else:  # when it's just a value
        return _value
    return d


# #### MAIN ###############
#
# # with open(conf_file, mode='r') as fin:
# #     prev_indent = 0
# #     for line in takewhile(lambda x: True, get_line(fin)):
# #         print line
#
#
# stuff = read_yaml_config(conf_file)
# print stuff