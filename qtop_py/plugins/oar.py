import logging
import os
import re
from collections import OrderedDict

from qtop_py import fileutils
from qtop_py import yaml_parser as yaml
from qtop_py.serialiser import GenericBatchSystem, StatExtractor

DEFAULT_NODE_STATE_MAPPING = {"Alive": "-", "Dead": "d", "Suspected": "s", "Mixed": "%", "Unknown": "?"}


class OarStatExtractor(StatExtractor):
    def __init__(self, config, options):
        StatExtractor.__init__(self, config, options)
        self.user_q_search = (
            r"^(?P<job_id>[0-9]+)\s+"
            r"(?:(?P<name>.*?)\s+)?"
            r"(?P<user>[0-9A-Za-z_.-]+)\s+"
            r"(?:\d{4}-\d{2}-\d{2})\s+"
            r"(?:\d{2}:\d{2}:\d{2})\s+"
            r"(?P<job_state>[A-Za-z])\s+"
            r"(?P<queue>\S+)"
        )

    def extract_qstat(self, orig_file):
        all_values = list()
        try:
            fileutils.check_empty_file(orig_file)
        except fileutils.FileEmptyError:
            logging.error("File %s seems to be empty." % orig_file)
            return all_values
        with open(orig_file, "r") as fin:
            logging.debug("File state before OarStatExtractor.extract_qstat: %(fin)s" % {"fin": fin})
            _ = fin.readline()  # header
            fin.readline()  # dashes
            re_match_positions = ("job_id", "user", "job_state", "queue")
            re_search = self.user_q_search
            for line in fin:
                if not line.strip():
                    continue
                try:
                    qstat_values = self._process_qstat_line(re_search, line, re_match_positions)
                except AttributeError as error:
                    raise ValueError("Cannot parse OAR job row: %s" % line.strip()) from error
                qstat_values["Queue"] = self.anonymize(qstat_values["Queue"], "qs")
                all_values.append(qstat_values)

        return all_values


class OARBatchSystem(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "oar"

    def __init__(self, scheduler_output_filenames, config, options):
        self.oarnodes_s_file = scheduler_output_filenames.get("oarnodes_s_file")
        self.oarnodes_y_file = scheduler_output_filenames.get("oarnodes_y_file")
        self.oarstat_file = scheduler_output_filenames.get("oarstat_file")

        self.config = config
        self.options = options
        self.oar_stat_maker = OarStatExtractor(self.config, self.options)

    def get_cluster_state(self, options):
        state = super().get_cluster_state(options)
        if self.diagnostics:
            state["oar_diagnostics"] = self.diagnostics.copy()
        return state

    def get_worker_nodes(self, job_ids, job_queues, options):
        nodes_resids = self._read_oarnodes_s_yaml(self.oarnodes_s_file)
        resids_jobs = self._read_oarnodes_y_textyaml(self.oarnodes_y_file)

        known_jobs = set(job_ids)
        listed_resources = {int(resid) for resources in nodes_resids.values() for resid, _ in resources}
        unlisted_jobs = {job for jobs in resids_jobs.values() for job in jobs} - known_jobs
        self.diagnostics = {
            "missing_resources": len(listed_resources - set(resids_jobs)),
            "extra_resources": len(set(resids_jobs) - listed_resources),
            "unlisted_jobs": len(unlisted_jobs),
            "shared_resources": sum(len(jobs) > 1 for jobs in resids_jobs.values()),
        }
        self.diagnostics = {key: count for key, count in self.diagnostics.items() if count}

        nodes_jobs = {}
        for node in nodes_resids:
            resids_state_lot = nodes_resids[node]
            for resid, state in resids_state_lot:
                nodes_jobs.setdefault(node, []).append((resids_jobs.get(int(resid), ["?"]), state))

        worker_nodes = list()
        node_state_mapping = dict(DEFAULT_NODE_STATE_MAPPING)
        node_state_mapping.update(self.config.get("oar_node_state_mappings", {}))
        for node in nodes_jobs:
            d = OrderedDict()
            d["domainname"] = node
            nr_of_jobs = len(nodes_jobs[node])
            d["np"] = nr_of_jobs
            d["core_job_map"] = {idx: next((job for job in jobs if job in known_jobs), jobs[0]) for idx, (jobs, _) in enumerate(nodes_jobs[node]) if jobs}
            d["oar_job_ids"] = list(OrderedDict.fromkeys(job for jobs, _ in nodes_jobs[node] for job in jobs if job != "?"))
            d["state"] = self._calculate_oar_state(nodes_jobs[node], node_state_mapping)
            worker_nodes.append(d)

        logging.info("worker_nodes contains %s entries" % len(worker_nodes))
        worker_nodes = self.ensure_worker_nodes_have_qnames(worker_nodes, job_ids, job_queues)
        return worker_nodes

    def get_jobs_info(self):
        qstats = self._get_jobs()
        records = [(str(qstat["JobId"]), qstat["UnixAccount"], qstat["S"], qstat["Queue"]) for qstat in qstats]
        job_ids, usernames, job_states, queue_names = map(list, zip(*records)) if records else ([], [], [], [])

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)}
        )
        return job_ids, usernames, job_states, queue_names

    def get_queues_info(self):
        """Derive job counts from oarstat; resource slots are not job counts."""
        queues = OrderedDict()
        for job in self._get_jobs():
            queue = queues.setdefault(job["Queue"], {"queue_name": job["Queue"], "run": 0, "queued": 0, "lm": "--", "state": "?"})
            queue["run"] += job["S"] == "R"
            queue["queued"] += job["S"] == "W"
        return sum(queue["run"] for queue in queues.values()), sum(queue["queued"] for queue in queues.values()), list(queues.values())

    def _get_jobs(self):
        if not hasattr(self, "_jobs"):
            self._jobs = self.oar_stat_maker.extract_qstat(self.oarstat_file)
        return self._jobs

    def _read_oarnodes_s_yaml(self, fn_s):
        if not os.path.isfile(fn_s):
            raise FileNotFoundError("OAR node input is not a regular file: %s" % fn_s)
        anonymize = self.oar_stat_maker.anonymize_func()
        logging.debug("File %s exists: %s" % (fn_s, os.path.isfile(fn_s)))
        if os.stat(fn_s).st_size == 0:
            raise ValueError("OAR node input is empty: %s" % fn_s)
        data = yaml.safe_load(fn_s, DEF_INDENT=4)
        if self.options.ANONYMIZE:
            nodes_resids = dict([(anonymize(node, "wns"), resid_state.items()) for node, resid_state in data.items()])
        else:
            nodes_resids = dict([(node, resid_state.items()) for node, resid_state in data.items()])
        return nodes_resids

    def _read_oarnodes_y_textyaml(self, fn):
        """Stream relevant YAML fields; blanks do not terminate resource records."""
        resources = {}
        resid = None
        jobs_list = False
        with open(fn) as stream:
            for line in stream:
                text = line.strip()
                if not text or text.startswith("#") or text in ("---", "..."):
                    continue
                header = re.fullmatch(r"([0-9]+):", line.rstrip())
                if header:
                    resid = int(header.group(1))
                    if resid in resources:
                        raise ValueError("Duplicate OAR resource ID %s" % resid)
                    resources[resid] = []
                    jobs_list = False
                elif resid is not None and text.startswith("jobs:"):
                    value = text.split(":", 1)[1].strip()
                    resources[resid] = self._parse_job_ids(value)
                    jobs_list = not value
                elif resid is not None and jobs_list and text.startswith("- "):
                    resources[resid].extend(self._parse_job_ids(text[2:]))
                else:
                    jobs_list = False
        return resources

    @staticmethod
    def _parse_job_ids(value):
        value = yaml.literal_or_original(value)
        if value is None or value in ("", "~", "null"):
            return []
        tokens = value if isinstance(value, (list, tuple)) else str(value).split(",")
        jobs = [str(token).strip() for token in tokens]
        if any(not re.fullmatch(r"[0-9]+", job) for job in jobs):
            raise ValueError("Invalid OAR resource job IDs")
        return list(OrderedDict.fromkeys(jobs))

    @staticmethod
    def _calculate_oar_state(jobid_state_lot, node_state_mapping):
        """
        If all resource ids within the node are either alive or dead or suspected, the respective label is given to the node.
        Otherwise, a mixed-state is reported
        """
        states = {job_state_tpl[1] for job_state_tpl in jobid_state_lot}
        if not states:
            return node_state_mapping["Unknown"]
        state = states.pop() if len(states) == 1 else "Mixed"
        return node_state_mapping.get(state, node_state_mapping["Unknown"])
