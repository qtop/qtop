__author__ = 'sfranky'

import os
from itertools import takewhile, dropwhile
# conf_file = '/home/sfranky/.local/qtop/qtopconf.yaml'


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
            block, line = read_yaml_config_block(line, fin, get_lines, block)
            # for key in block:
            #     print key
            #     print '\t' + str(block[key])
            raw_key_values.update(block)

        config_dict = dict([(key, value) for key, value in raw_key_values.items()])
        return config_dict


def read_yaml_config_block(line, fin, get_lines, block):
    last_empty_container = block

    if len(line) > 1:  # non-empty line
        key_value, last_empty_container = process_line(line, fin, get_lines, last_empty_container)
        for (k, v) in key_value.items():
            block[k] = v

    while len(line) == 1:  # skip empty lines
        line = next(get_lines)

    while len(line) > 1:  # as long as a blank line is not reached (i.e. block is not complete)
        if line[0] == 0:  # same level
            key_value, new_container = process_line(line, fin, get_lines, last_empty_container)
            for k in key_value:
                if k == '-':
                    _list.append(new_container)
                else:
                    last_empty_container[k] = key_value[k]  # fill up parent container with new key_value
                    last_empty_container = new_container  # point towards latest container (key_value's value)
        elif line[0] > 0:  # nesting
            key_value, new_container = process_line(line, fin, get_lines, last_empty_container)
            for k in key_value:
                last_empty_container[k] = key_value[k]
                if k == '-':
                    _list = last_empty_container[k]
                last_empty_container = new_container

        elif line[0] < 0:  # go up one level
            key_value, container = process_line(line, fin, get_lines, last_empty_container)
            for k in key_value:
                if k == '-':
                    _list.extend(key_value[k])
                last_empty_container = container
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


def process_line(list_line, fin, get_lines, last_empty_container):
    key = list_line[1]
    if len(list_line) == 2:  # key-only, so what's in the line following should be written in a new container
        container = {}
        return {key: container}, container
    elif len(list_line) == 3:
        container = list_line[2]
        if ': ' in container:  # key must have been '-'
            parent_key = key
            key, container = container.split(None, 1)
            return {parent_key: [{key: container}]}, last_empty_container
        elif container.endswith(':'):  # key must have been '-'
            parent_key = key
            key = container
            container = {}  # new container
            return {parent_key: [{key: container}]}, container
        else:  # simple value
            if key == '-':
                last_empty_container = container
            return {key: container}, last_empty_container
    else:
        raise ValueError("Didn't anticipate that!")


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