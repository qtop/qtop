import os
import logging


## TODO: black sheep

def fix_config_list(config_list):
    """
    transforms a list of the form ['a, b'] to ['a', 'b']
    """
    if not config_list:
        return []
    t = config_list
    item = t[0]
    list_items = item.split(',')
    return [nr.strip() for nr in list_items]


def get_line(fin, verbatim=False, SEPARATOR=None, DEF_INDENT=2):
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

    DEF_INDENT is how many spaces is an indent by default.
    e.g. qtop config file uses 2 spaces, oarnodes_s_y uses 4
    """
    indent = 0
    indenter = {
        0: 0,
        DEF_INDENT / 2: 1,
        DEF_INDENT: 1,
        3 * int(float(DEF_INDENT) / 2): 2,
        2 * DEF_INDENT: 2,
        -DEF_INDENT / 2: -1,
        -DEF_INDENT: -1,
        -3 * int(float(DEF_INDENT) / 2): -2,
        - 2 * DEF_INDENT: -2,
    }
    for line in fin:
        if line.lstrip().startswith('#') or line.strip() == '---':
            continue
        elif ' #' in line and not line.endswith('#\n'):
            line = line.split(' #', 1)[0]

        prev_indent = indent
        indent = len(line) - len(line.lstrip(' '))
        diff = indent - prev_indent
        try:
            d_indent = indenter[diff]
        except KeyError:
            d_indent = diff

        line = line.rstrip()
        list_line = verbatim and [d_indent, line] or [d_indent] + line.split(None or SEPARATOR, 1)

        if len(list_line) > 1:
            if list_line[1].startswith(('"', "'")):
                list_line[1] = list_line[1][1:-1]
        else:
            pass
        yield list_line


def convert_dash_key_in_dict(d):
    """
    takes a dict of the form {'-': [...]} and converts it to [...]
    """
    try:
        assert isinstance(d, dict)
    except AssertionError:
        return d  # TODO: Maybe this should fail, not be muted

    for key_out in d:
        if not (isinstance(d[key_out], dict) or len(d[key_out]) == 1):
            continue
        try:
            for key_in in d[key_out]:
                if key_in == '-' and key_out != 'state':
                    d[key_out] = d[key_out][key_in]
                # elif key_in == '-' and key_out == 'state':
                #     d[key_out] = eval(d[key_out])
                #     break
        except TypeError:
            return d
        except IndexError:
            continue
    return d


def parse(fn, DEF_INDENT=2):
    raw_key_values = {}
    with open(fn, mode='r') as fin:
        try:
            assert os.stat(fn).st_size != 0
        except AssertionError:
            logging.critical('File %s is empty!! Exiting...\n' % fn)
            raise
        except IOError:
            raise
        logging.debug('File state before parse: %s' % fin)
        get_lines = get_line(fin, DEF_INDENT=DEF_INDENT)  # TODO: weird
        line = next(get_lines)
        while line:
            block, line = read_yaml_config_block(line, fin, get_lines)
            block = convert_dash_key_in_dict(block)
            for k in block:
                block[k] = convert_dash_key_in_dict(block[k])
            raw_key_values.update(block)

    logging.debug('File state after parse: %s' % fin)
    a_dict = dict([(key, value) for key, value in raw_key_values.items()])
    return a_dict


def read_yaml_config_block(line, fin, get_lines):
    block = dict()
    parent_container = block
    open_containers = list()
    open_containers.append(block)

    # if len(line) > 1:  # non-empty line
    #     key_value, parent_container = process_line(line, fin, get_lines, parent_container)
    #     for (k, v) in key_value.items():
    #         block[k] = v

    while len(line) == 1:  # skip empty lines
        try:
            line = next(get_lines)
        except StopIteration:  # EO(config)F
            return {}, ''

    while len(line) > 1:  # as long as a blank line is not reached (i.e. block is not complete)
        # if line[0] == 0 or (line[0] != 0 and line[1] == '-'):  # same level
        # key_value used below belongs to previous line. It will work for first block line because of short circuit logic
        if line[0] == 0 \
                or (line[0] == 1 and (key_value.keys()[0] == '-'))\
                or (line[0] == -1 and line[1] == '-'):  # same level or entry level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            for k in key_value:
                pass  # assign dict's sole key to k
            if parent_container == {} or '-' not in parent_container:
                parent_container[k] = key_value[k]
            elif '-' in parent_container and '-' not in key_value:
                last_item = parent_container['-'].pop()
                key_value.update(last_item)
                parent_container['-'].append(key_value)
            else:
                parent_container.setdefault(k, []).extend(key_value[k])  # was waiting for a list, but a str came in!
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)

        elif (line[0] == 1) or (line[0] > 1):  # go down one level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            for k in key_value:
                pass
            # if container == {}:  # up parent container with new value
            if parent_container == {}:  # above it is a key waiting to be filled with values
                parent_container[k] = key_value[k]
            else:
                parent_container.setdefault(k, []).append(key_value[k]) if isinstance(key_value[k], str) else \
                    parent_container.setdefault(k, []).extend(key_value[k])
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)

        elif line[0] == -2 and line[1] == '-':  # go up two levels
            key_value, container = process_line(line, fin, get_lines, parent_container)
            len(open_containers) > 1 and open_containers.pop() or None
            for k in key_value:
                pass  # assign dict's sole key to k
            if open_containers[-1].get('-'):
                open_containers[-1].setdefault('-', []).extend(key_value[k])
            else:
                open_containers[-1][k] = key_value[k]
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)
            else:
                parent_container = open_containers[-1]

        elif line[0] == -1:  # go up one level
            key_value, container = process_line(line, fin, get_lines, parent_container)
            len(open_containers) > 1 and open_containers.pop() or None
            for k in key_value:
                pass  # assign dict's sole key to k
            if open_containers[-1].get('-'):
                open_containers[-1].setdefault('-', []).extend(key_value[k])
            else:
                open_containers[-1][k] = key_value[k]
            if container == {}:
                open_containers.append(container)
                parent_container = open_containers[-1]  # point towards latest container (key_value's value)
            else:
                parent_container = open_containers[-1]

        try:
            line = next(get_lines)
        except StopIteration:
            return block, ''
        else:
            if line[-1] == '...':
                return block, line
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
            # container = [container[1:-1]] if container.startswith('[') else container
            container = container[1:-1].split(', ') if container.startswith('[') else container
            container = "" if container in ("''", '""') else container
            if len(container) == 1 and isinstance(container, list) and isinstance(container[0], str):
                try:
                    container = list(eval(container[0]))
                except NameError:
                    pass
            return {'-': [{key.rstrip(':'): container}]}, container  #list

        elif container.endswith('|'):
            container = process_code(fin)
            return {key.rstrip(':'): container}, parent_container

        else:  # simple value
            if key == '-':  # i.e.  - testvalue
                return {'-': [container]}, container  # was parent_container******was :[container]}, container
            else:  # i.e. testkey: testvalue
                container = [container[1:-1]] if container.startswith('[') else container  #list
                if len(container) == 1 and isinstance(container, list) and isinstance(container[0], str):
                    try:
                        container = list(eval(container[0]))
                    except NameError:
                        pass
                elif container.startswith("'") and container.endswith("'"):
                    container = eval(container)
                return {key.rstrip(':'): container}, container  # was parent_container#str
    else:
        raise ValueError("Didn't anticipate that!")


def process_code(fin):
    get_code = get_line(fin, verbatim=True)
    # line = next(get_code)
    line = next(get_code)
    code = []
    while line[0] > -1:
        code.append(' ' + line[-1])
        try:
            line = next(get_code)
        except StopIteration:
            break
    return '\n'.join([c.strip() for c in code]).strip()


def safe_load(fin, DEF_INDENT=2):
    a_dict = parse(fin, DEF_INDENT)
    logging.debug("YAML dict length: %s" % len(a_dict))
    return a_dict


def load_all(fin):
    list_of_dicts = []
    get_lines = get_line(fin)
    while True:
        try:
            line = next(get_lines)
        except StopIteration:
            break
        block, line = read_yaml_config_block(line, fin, get_lines)
        block = convert_dash_key_in_dict(block)
        list_of_dicts.append(block)

    return list_of_dicts


def get_yaml_key_part(config, scheduler, outermost_key):
    """
    only return the list items of the yaml outermost_key if a yaml key subkey exists
    (this signals a user-inserted value)
    """
    # e.g. outermost_key = 'workernodes_matrix'
    for part in config[outermost_key]:
        part_name = [i for i in part][0]
        part_options = part[part_name]
        yaml_key = part_options.get('yaml_key')
        # if no systems line exists, all systems are supported, and thus the current
        systems = fix_config_list(part_options.get('systems', [scheduler]))
        if yaml_key:
            yield yaml_key, part_name, systems
