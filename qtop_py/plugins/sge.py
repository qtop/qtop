##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2026 Jacob Hatchett
##
## SPDX-License-Identifier: MIT
##
"""SGE backend: parses qstat -xml output."""

__author__ = "sfranky"
import fnmatch
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, cast

# SGE parses with NoDTDTreeBuilder below, rejecting all DTD declarations.
from xml.etree import ElementTree as etree  # nosec B405

import qtop_py.fileutils as fileutils
from qtop_py import safety
from qtop_py.errors import OperationalError
from qtop_py.serialiser import GenericBatchSystem, StatExtractor

if TYPE_CHECKING:
    from qtop_py.source_trace import TraceMasker
    from qtop_py.state_types import Config, JobsInfo, Node, Options, QueuesInfo, RawQueue


def _required_text_element(parent: etree.Element, path: str) -> etree.Element:
    element = parent.find(path)
    if element is None or not element.text:
        raise etree.ParseError("SGE XML requires a nonempty %s element" % path)
    return element


def _required_text(parent: etree.Element, path: str) -> str:
    return _required_text_element(parent, path).text or ""  # nonempty, checked there


def _anonymize_field(parent: etree.Element, path: str, anonymize: Callable[[str, str], str], category: str) -> str:
    element = _required_text_element(parent, path)
    text = anonymize(element.text or "", category)  # element.text is nonempty, checked above
    element.text = text
    return text


def _job_id(parent: etree.Element, anonymize: Callable[[str, str], str]) -> str:
    job_id = _anonymize_field(parent, "./JB_job_number", anonymize, "jobnums")
    tasks = (parent.findtext("./tasks") or "").strip()
    return "%s.%s" % (job_id, tasks) if tasks else job_id


class NoDTDTreeBuilder(etree.TreeBuilder):
    """Reject DTDs before scheduler XML can expand declared entities."""

    def __init__(self) -> None:
        super().__init__()
        self.depth = self.elements = 0

    def start(self, tag: str, attrs: Dict[str, str]) -> etree.Element:
        """Open an element; raise ParseError beyond MAX_DEPTH nesting or MAX_ITEMS elements."""
        self.depth += 1
        self.elements += 1
        if self.depth > safety.MAX_DEPTH or self.elements > safety.MAX_ITEMS:
            raise etree.ParseError("XML input exceeds nesting or element limits")
        return super().start(tag, attrs)

    def end(self, tag: str) -> etree.Element:
        """Close an element."""
        self.depth -= 1
        return super().end(tag)

    def doctype(self, name: str, pubid: str, system: str, *_internal_subset: Any) -> None:
        """Reject any DTD declaration."""
        raise etree.ParseError("DTD declarations are not supported in scheduler XML")


def _xml_parser() -> etree.XMLParser:
    target = NoDTDTreeBuilder()
    parser = etree.XMLParser(target=target)  # nosec B314 - DTD rejection below
    # Pure-Python ElementTree (PyPy) does not dispatch every DTD to the target.
    expat_parser = getattr(parser, "_parser", None)
    if expat_parser is not None:
        expat_parser.StartDoctypeDeclHandler = target.doctype
    return parser


class SGEStatExtractor(StatExtractor):
    """Parse SGE qstat -xml output, keeping queues listed in sge_whole_queues whole."""

    def __init__(self, config: "Config", options: "Options", scheduler_output_filenames: Dict[str, str]) -> None:
        StatExtractor.__init__(self, config, options)
        self.scheduler_output_filenames = scheduler_output_filenames
        # The in-house YAML parser yields flow lists as one comma-joined string; queue names never contain commas.
        self.whole_queue_patterns = [p.strip() for item in config.get("sge_whole_queues") or () for p in item.split(",") if p.strip()]
        # Queue-List elements matching sge_whole_queues, recorded before anonymization rewrites their names.
        self.whole_queue_elems: Set[etree.Element] = set()

    def anonymize_queue_list_nametag(self, queue_list_nametag: etree.Element) -> str:
        """Return queue@node with both parts anonymised."""
        name, nodename = (queue_list_nametag.text or "").split("@")  # nonempty, checked by the caller
        name = self.anonymize(name, "qs")
        nodename = self.anonymize(nodename, "wns")
        return name + "@" + nodename

    def get_xml_tree(self, xml_file: str) -> Tuple[etree.ElementTree, Any]:
        """Parse an XML file within the size and nesting limits, rejecting DTDs; return the tree and root."""
        with open(xml_file, mode="rb") as fin:
            try:
                # Bandit cannot infer DTD rejection by the custom parser target.
                parser = _xml_parser()
                total = 0
                for chunk in iter(lambda: fin.read(65536), b""):
                    total += len(chunk)
                    if total > safety.MAX_INPUT_BYTES:
                        raise etree.ParseError("XML input exceeds size limit")
                    parser.feed(chunk)
                tree = etree.ElementTree(parser.close())
            except etree.ParseError:
                logging.critical("Something happened during the parsing of the XML file. Exiting...")
                raise
            except IOError:
                raise
            except Exception as error:
                logging.debug("XML file state %s" % fin, exc_info=True)
                raise OperationalError("Cannot read SGE XML %s: %s" % (xml_file, error)) from error
            else:
                root = tree.getroot()
        return tree, root

    def extract_qstat(self, orig_file: str) -> List[Any]:
        """Parse qstat -xml output into job records."""
        all_values: List[Any] = list()
        self.orig_file = orig_file
        self.whole_queue_elems = set()
        self.tree, self.root = self.get_xml_tree(orig_file)
        root = self.root

        for queue_elem in root.findall("queue_info/Queue-List"):
            queue_name_elems = queue_elem.findall("resource")
            queue_list_nametag = _required_text_element(queue_elem, "name")
            queue_list_nametag.text = self.anonymize_queue_list_nametag(queue_list_nametag)
            _q_name_elems = iter(queue_name_elems)
            for queue_name_elem in _q_name_elems:
                if queue_name_elem.attrib.get("name") == "qname":
                    if not queue_name_elem.text:
                        raise etree.ParseError("SGE XML requires a nonempty qname resource")
                    if any(fnmatch.fnmatchcase(queue_name_elem.text, pattern) for pattern in self.whole_queue_patterns):
                        self.whole_queue_elems.add(queue_elem)
                    queue_name_elem.text = self.anonymize(queue_name_elem.text, "qs")
                    next_q_name_elem = next(_q_name_elems, None)
                    if next_q_name_elem is None or not next_q_name_elem.text:
                        raise etree.ParseError("SGE XML requires a nonempty hostname resource")
                    # if queue_name_elem.attrib.get('name') == 'hostname': # assume next must be hostname?
                    next_q_name_elem.text = self.anonymize(next_q_name_elem.text, "wns")
                    break
            else:
                raise ValueError("No such queue name")

            try:
                all_values = self._extract_job_info(all_values, queue_elem, "job_list", queue_name=queue_name_elem.text)
            except ValueError:
                logging.warning("No jobs found in XML file!")

        # look for the remaining, pending jobs, found later in the xml file
        job_info_elem = root.find("./job_info")
        if job_info_elem is None:
            logging.debug("No pending jobs found!")
        else:
            try:
                all_values = self._extract_job_info(all_values, job_info_elem, "job_list", queue_name="Pending")
            except ValueError:
                logging.warning("No jobs found in XML file!")

        return all_values

    def _extract_job_info(self, all_values: List[Any], elem: etree.Element, elem_text: str, queue_name: str) -> List[Any]:
        """Extract scheduler job records from an XML element into ``all_values``."""
        for subelem in elem.findall(elem_text):
            owner = _anonymize_field(subelem, "./JB_owner", self.anonymize, "users")
            job_num = _job_id(subelem, self.anonymize)
            _anonymize_field(subelem, "./JB_name", self.anonymize, "jobnames")
            subm_time = subelem.find("./JB_submission_time")
            if subm_time is not None and subm_time.text:
                _anonymize_field(subelem, "./JB_submission_time", self.anonymize, "jobtimes")
            queue_name = self.anonymize(queue_name, "qs")
            qstat_values: Dict[str, Any] = dict()
            qstat_values["JobId"] = job_num
            qstat_values["UnixAccount"] = owner
            qstat_values["S"] = _required_text(subelem, "./state")
            qstat_values["Queue"] = queue_name
            all_values.append(qstat_values)
        if not all_values:
            raise ValueError("No jobs found in XML file!")

        return all_values


class SGEBatchSystem(GenericBatchSystem):
    """SGE backend."""

    native_queue_membership = True
    blocked_queue_note = " (at least one queue instance)"

    @staticmethod
    def queue_is_blocked(state: object) -> bool:
        """Return True if the queue state carries any SGE alarm, suspend, disable, error or unknown flag."""
        return any(flag in str(state) for flag in "aAcCdDEosSu")

    def mask_source_trace(self, key: str, text: str, mask: "TraceMasker") -> Tuple[str, str]:
        """Return the masked name and XML text of a qstat -xml trace."""
        if key != "sge_file":
            raise ValueError("unsupported trace")
        parser = _xml_parser()
        root = etree.fromstring(text, parser=parser)  # nosec B314
        numeric = {"slots_used", "slots_resv", "slots_total", "slots", "tasks", "JAT_prio", "JB_job_number"}
        resources = {"num_proc", "slots", "slots_total", "slots_used", "slots_resv"}
        tag_kinds, resource_kinds = {"JB_job_number": "job", "JB_owner": "user"}, {"hostname": "node", "qname": "queue"}
        tags = numeric | {
            "job_info",
            "queue_info",
            "Queue-List",
            "job_list",
            "name",
            "qtype",
            "arch",
            "state",
            "resource",
            "JB_owner",
            "JB_name",
            "JB_submission_time",
            "JAT_start_time",
        }
        for element in root.iter():
            if element.tag not in tags or set(element.attrib) - {"name", "state", "type"}:
                raise ValueError("unsupported XML field")
            resource = element.attrib.get("name", "") if element.tag == "resource" else ""
            value = element.text or ""
            kind = tag_kinds.get(element.tag) or resource_kinds.get(resource)
            if value.strip():
                if kind:
                    element.text = mask(value, kind)
                elif element.tag == "name" and "@" in value:
                    queue, host = value.split("@", 1)
                    element.text = mask(queue, "queue") + "@" + mask(host, "node")
                elif element.tag in numeric or resource in resources:
                    element.text = mask.public(value, r"[0-9.,:\- ]+")
                elif element.tag == "state":
                    element.text = mask.public(value, r"[rRqQwWhHsStTEdauocACD+-]*")
                else:
                    element.text = mask(value)
            for name, value in list(element.attrib.items()):
                if name == "name" and resource in resources | {"hostname", "qname"}:
                    continue
                if name == "state" and value in ("running", "pending", "finished"):
                    continue
                element.attrib[name] = mask(value)
            if element.tail and element.tail.strip():
                element.tail = mask(element.tail)
        return "qstat.F.xml.stdout", etree.tostring(root, encoding="unicode")

    @staticmethod
    def get_mnemonic() -> str:
        """Return the scheduler name, sge."""
        return "sge"

    def __init__(self, scheduler_output_filenames: Dict[str, str], config: "Config", options: Any) -> None:
        # "" (falsy, like the former None) when a trace is not configured; reading it then fails as an OSError.
        self.sge_file = scheduler_output_filenames.get("sge_file", "")
        self.config = config
        self.options = options
        self.sge_stat_maker = SGEStatExtractor(self.config, self.options, scheduler_output_filenames)
        if self.options.ANONYMIZE:
            self.anonymize = self.sge_stat_maker.anonymize_func()
        else:
            self.anonymize = self.sge_stat_maker.eponymize_func()

    def get_queues_info(self) -> "QueuesInfo":
        """Return the running and queued totals and per-queue records from qstat -xml."""
        logging.debug("Parsing tree of %s" % self.sge_file)
        fileutils.check_empty_file(self.sge_file)

        root = self.sge_stat_maker.root

        qstatq_list = self._extract_queues("queue_info/Queue-List", root)

        total_running_jobs = sum(int(d["run"]) for d in qstatq_list)
        logging.info("Total running jobs found: %s" % total_running_jobs)

        for d in qstatq_list:
            d["run"] = str(d["run"])
            d["queued"] = str(d["queued"])

        total_queued_jobs = self._get_total_queued_jobs("job_info/job_list", root)

        qstatq_list.append({"run": "0", "queued": total_queued_jobs, "queue_name": "Pending", "state": "-", "lm": "0"})
        logging.debug("qstatq_list contains %s elements" % len(qstatq_list))
        logging.debug("Closing %s" % self.sge_file)

        queues: "List[RawQueue]" = cast("List[RawQueue]", qstatq_list)  # every row is a queue record
        return total_running_jobs, int(total_queued_jobs), queues

    def get_worker_nodes(self, job_ids: List[str], job_queues: List[str], options: "Options") -> "List[Node]":
        """Return the nodes, with their queues and slots, from qstat -xml."""
        logging.debug("Parsing tree of %s" % self.sge_file)

        tree, root = self.sge_stat_maker.tree, self.sge_stat_maker.root

        existing_wns: List[Dict[str, Any]] = list()
        existing_node_names = set()
        whole_queue_jobs: Dict[str, Any] = dict()

        for queue_elem in root.findall("queue_info/Queue-List"):
            worker_node = self._get_host_qname_np(queue_elem)
            worker_node["state"] = self._get_state(queue_elem)

            if worker_node["domainname"] not in existing_node_names:
                job_ids, _, _ = self._extract_running_job_info(queue_elem, "job_list")
                if queue_elem in self.sge_stat_maker.whole_queue_elems and job_ids:
                    whole_queue_jobs.setdefault(worker_node["domainname"], job_ids[0])
                worker_node["core_job_map"] = dict((idx, job_id) for idx, job_id in enumerate(job_ids))
                worker_node["existing_busy_cores"] = len(worker_node["core_job_map"])
                worker_node["np"] = max(int(worker_node["np"]), len(worker_node["core_job_map"]))

                existing_node_names.update([worker_node["domainname"]])
                existing_wns.append(worker_node)
            else:
                for existing_wn in existing_wns:
                    if worker_node["domainname"] != existing_wn["domainname"]:
                        continue

                    job_ids, _, _ = self._extract_running_job_info(queue_elem, "job_list")
                    if queue_elem in self.sge_stat_maker.whole_queue_elems and job_ids:
                        whole_queue_jobs.setdefault(worker_node["domainname"], job_ids[0])
                    core_jobs = dict((idx, job_id) for idx, job_id in enumerate(job_ids, existing_wn["existing_busy_cores"]))
                    existing_wn["core_job_map"].update(core_jobs)
                    existing_wn["existing_busy_cores"] = len(existing_wn["core_job_map"])
                    # don't change the node state to free.
                    # Just keep the state reported in the last queue mentioning the node.
                    existing_wn["state"] = existing_wn["state"] if worker_node["state"] == "-" else worker_node["state"]
                    existing_wn["qname"].update(worker_node["qname"])
                    existing_wn["np"] = max(int(existing_wn["np"]), len(existing_wn["core_job_map"]))
                    break

        for existing_wn in existing_wns:
            self._expand_whole_queue_node(existing_wn, whole_queue_jobs.get(existing_wn["domainname"]))

        logging.debug("Closing %s" % self.sge_file)
        logging.info("existing_wns contains %s entries" % len(existing_wns))
        for existing_wn in existing_wns:
            existing_wn["qname"] = list(existing_wn["qname"])

        # last to be reading the xml file, can now write back if anonymizing..
        if self.options.SAMPLE >= 1:
            anon_file = self.sge_stat_maker.orig_file + "%s" % ("_anon" if self.options.ANONYMIZE else "")
            self.sge_stat_maker.scheduler_output_filenames["sge_file"] = anon_file
            tree.write(anon_file)
        return cast("List[Node]", existing_wns)  # built field by field as Node records

    def get_jobs_info(self) -> "JobsInfo":
        """Return job IDs, users, states and queues from qstat -xml."""
        all_values = self.sge_stat_maker.extract_qstat(self.sge_file)
        records = [(str(qstat["JobId"]), qstat["UnixAccount"], qstat["S"], qstat["Queue"]) for qstat in all_values]
        job_ids, usernames, job_states, queue_names = map(list, zip(*records)) if records else ([], [], [], [])

        logging.debug(
            "job_ids, usernames, job_states, queue_names lengths: "
            "%(job_ids)s, %(usernames)s, %(job_states)s, %(queue_names)s"
            % {"job_ids": len(job_ids), "usernames": len(usernames), "job_states": len(job_states), "queue_names": len(queue_names)}
        )
        return job_ids, usernames, job_states, queue_names

    @staticmethod
    def _expand_whole_queue_node(worker_node: Dict[str, Any], job_id: Optional[str]) -> None:
        """A running job in a sge_whole_queues queue owns its whole node: fill the free cores, keep other jobs' cores."""
        if job_id is None:
            return
        core_job_map = worker_node["core_job_map"]
        for core in range(int(worker_node["np"])):
            core_job_map.setdefault(core, job_id)
        worker_node["existing_busy_cores"] = len(core_job_map)

    def _extract_running_job_info(self, elem: etree.Element, elem_text: str) -> Tuple[List[str], List[str], List[str]]:
        """Extract running jobs used to construct a worker node's core map."""
        job_ids, usernames, job_states = [], [], []
        for subelem in elem.findall(elem_text):
            state = subelem.get("state")
            if state != "running":
                continue

            owner = _anonymize_field(subelem, "./JB_owner", self.anonymize, "users")
            job_num = _job_id(subelem, self.anonymize)
            _anonymize_field(subelem, "./JB_name", self.anonymize, "jobnames")
            _anonymize_field(subelem, "./JAT_start_time", self.anonymize, "jobtimes")
            slots_text = (subelem.findtext("./slots") or "").strip() or "1"
            try:
                slots = int(slots_text)
            except ValueError as error:
                raise etree.ParseError("SGE XML requires positive integer slots, got %r" % slots_text) from error
            if slots < 1:
                raise etree.ParseError("SGE XML requires positive integer slots, got %r" % slots_text)
            job_ids.extend([job_num] * slots)
            usernames.extend([owner] * slots)
            job_states.extend([_required_text(subelem, "./state")] * slots)
        return job_ids, usernames, job_states

    def _get_host_qname_np(self, queue_elem: etree.Element) -> Dict[str, Any]:
        worker_node: Dict[str, Any] = dict()
        count = 0

        resources = queue_elem.findall("resource")
        for resource in resources:
            if resource.attrib.get("name") == "hostname":
                worker_node["domainname"] = self.anonymize(cast(str, resource.text), "wns")  # a missing text passes through, as before
                count += 1
            elif resource.attrib.get("name") == "qname":
                qname = resource.text
                worker_node["qname"] = set([self.anonymize(cast(str, qname), "qs")])  # likewise
                count += 1
            elif resource.attrib.get("name") == "num_proc":
                # Treat an implausible core count like a missing one, before anything loops over it.
                if safety.cores_per_node(resource.text) is None:
                    logging.warning("Ignoring invalid SGE num_proc %r (expected 0..%d)", resource.text, safety.MAX_CORES_PER_NODE)
                    worker_node["np"] = 0
                else:
                    worker_node["np"] = resource.text
                count += 1
            if count == 3:
                break
        else:
            # out of the 3, np information is the most likely to be missing from a node
            worker_node["np"] = 0

        return worker_node

    def _get_state(self, queue_elem: etree.Element) -> str:
        return queue_elem.findtext("state", default="-") or "-"

    def _extract_queues(self, xpath: str, root: etree.Element) -> List[Dict[str, Any]]:
        qstatq_list: List[Dict[str, Any]] = []
        for queue_elem in root.findall(xpath):
            queue_names = queue_elem.findall("resource")
            for _queue_name in queue_names:
                if _queue_name.attrib.get("name") == "qname":
                    queue_name = self.anonymize(cast(str, _queue_name.text), "qs")  # likewise
                    break
            else:
                raise ValueError("No such resource")

            for exist_d in qstatq_list:
                if queue_name == exist_d["queue_name"]:
                    jobs = queue_elem.findall("job_list")
                    run_count = 0
                    for _run in jobs:
                        if _run.attrib.get("state") == "running":
                            run_count += 1
                    exist_d["run"] = int(exist_d["run"]) + run_count
                    state = queue_elem.findtext("./state", default="?") or "?"
                    exist_d["state"] = "".join(sorted(set(str(exist_d["state"]) + state)))
                    break
            else:  # first instance of queue in the xml
                d: Dict[str, Any] = dict()
                d["queue_name"] = queue_name
                d["state"] = queue_elem.findtext("./state", default="?") or "?"

                job_lists = queue_elem.findall("job_list")
                run_count = 0
                for _run in job_lists:
                    if _run.attrib.get("state") == "running":
                        run_count += 1
                d["run"] = run_count
                d["lm"] = 0
                d["queued"] = 0
                qstatq_list.append(d)

        return qstatq_list

    def _get_total_queued_jobs(self, xpath: str, root: etree.Element) -> str:
        total_queued_jobs_elems = root.findall(xpath)
        pending_count = 0
        for job in total_queued_jobs_elems:
            if job.attrib.get("state") == "pending":
                pending_count += 1
        total_queued_jobs = str(pending_count)
        logging.info("Total queued jobs found: %s" % total_queued_jobs)

        return total_queued_jobs
