import yaml
try:
    import ujson as json
except ImportError:
    import json
from tempfile import mkstemp
import os


try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def read_qstat_yaml(fn, write_method):
    """
    reads qstat YAML file and populates four lists. Returns the lists
    """
    job_ids, usernames, job_states, queue_names = [], [], [], []

    with open(fn) as fin:
        qstats = (write_method.endswith('yaml')) and yaml.load_all(fin, Loader=Loader) or json.load(fin)
        for qstat in qstats:
            job_ids.append(str(qstat['JobId']))
            usernames.append(qstat['UnixAccount'])
            job_states.append(qstat['S'])
            queue_names.append(qstat['Queue'])
    os.remove(fn)
    return job_ids, usernames, job_states, queue_names


def get_new_temp_file(suffix, prefix):  # **kwargs
    fd, temp_filepath = mkstemp(suffix=suffix, prefix=prefix)  # **kwargs
    # out_file = os.fdopen(fd, 'w')
    return fd, temp_filepath
    # return out_file