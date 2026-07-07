##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## SPDX-License-Identifier: MIT
##

"""Differential debugging helpers for comparing two qtop scheduler inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
from collections import Counter, OrderedDict
from collections.abc import Iterable as IterableABC
from collections.abc import Mapping as MappingABC
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import qtop_py.yaml_parser as yaml
from qtop_py.plugins.demo import DemoBatchSystem
from qtop_py.plugins.oar import OARBatchSystem
from qtop_py.plugins.pbs import PBSBatchSystem
from qtop_py.plugins.sge import SGEBatchSystem
from qtop_py.plugins.slurm import SlurmBatchSystem


BATCH_SYSTEMS = OrderedDict(
    (batch_system.get_mnemonic(), batch_system)
    for batch_system in (DemoBatchSystem, OARBatchSystem, PBSBatchSystem, SGEBatchSystem, SlurmBatchSystem)
)
ANSI_ESCAPE_RE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|.)?")


class DiffInputError(ValueError):
    """Raised when the diff command cannot build a cluster snapshot."""


@dataclass(frozen=True)
class DiffOptions:
    """Subset of qtop runtime options consumed by scheduler plugins."""

    ANONYMIZE: bool = False
    SAMPLE: int = 0
    SOURCEDIR: Optional[str] = None


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    user: str
    state: str
    queue: str


@dataclass(frozen=True)
class QueueRecord:
    name: str
    running: int
    queued: int
    limit: str
    state: str


@dataclass(frozen=True)
class NodeRecord:
    name: str
    state: str
    cores: int
    busy_cores: int
    queues: Tuple[str, ...]
    jobs: Tuple[str, ...]


@dataclass(frozen=True)
class ClusterSnapshot:
    name: str
    scheduler: str
    source: str
    jobs: Mapping[str, JobRecord]
    queues: Mapping[str, QueueRecord]
    nodes: Mapping[str, NodeRecord]
    reported_running_jobs: int
    reported_queued_jobs: int

    def metrics(self) -> Mapping[str, int]:
        node_states = Counter(node.state for node in self.nodes.values())
        return OrderedDict(
            (
                ("nodes_total", len(self.nodes)),
                ("nodes_down", node_states.get("d", 0)),
                ("nodes_unknown", node_states.get("?", 0)),
                ("cores_total", sum(node.cores for node in self.nodes.values())),
                ("cores_busy", sum(node.busy_cores for node in self.nodes.values())),
                ("jobs_total", len(self.jobs)),
                ("jobs_reported_running", int(self.reported_running_jobs)),
                ("jobs_reported_queued", int(self.reported_queued_jobs)),
                ("queues_total", len(self.queues)),
                ("users_total", len(set(job.user for job in self.jobs.values()))),
            )
        )


@dataclass(frozen=True)
class ComparisonRow:
    key: str
    left: str
    right: str
    status: str


@dataclass(frozen=True)
class DiffReport:
    left: ClusterSnapshot
    right: ClusterSnapshot
    metrics: Tuple[ComparisonRow, ...]
    queues: Tuple[ComparisonRow, ...]
    nodes: Tuple[ComparisonRow, ...]
    jobs: Tuple[ComparisonRow, ...]

    @property
    def has_differences(self) -> bool:
        all_rows = self.metrics + self.queues + self.nodes + self.jobs
        return any(row.status != "same" for row in all_rows)


class Ansi:
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    dim = "\033[2m"
    reset = "\033[0m"


def default_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "qtopconf.yaml"


def load_config(config_path: Optional[Path]) -> Dict[str, Any]:
    default_path = default_config_path()
    if not default_path.exists():
        raise DiffInputError("Default qtop configuration was not found: %s" % default_path)

    try:
        config = yaml.parse(str(default_path)) or {}
        overlay = None
        if config_path is not None:
            overlay = yaml.parse(str(config_path)) or {}
    except Exception as exc:
        raise DiffInputError("Cannot parse configuration YAML: %s" % exc) from exc

    if not isinstance(config, MappingABC):
        raise DiffInputError("Default qtop configuration must be a mapping: %s" % default_path)
    if overlay is not None:
        if not isinstance(overlay, MappingABC):
            raise DiffInputError("qtop configuration overlay must be a mapping: %s" % config_path)
        config = _deep_merge(config, overlay)
    return config


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, MappingABC) and isinstance(overlay_value, MappingABC):
            merged[key] = _deep_merge(base_value, overlay_value)
        else:
            merged[key] = overlay_value
    return merged


def scheduler_files_for_source(source_dir: Path, scheduler: str, config: Mapping[str, Any]) -> Mapping[str, str]:
    try:
        schedulers = config["schedulers"]
        if not isinstance(schedulers, MappingABC):
            raise TypeError("qtop schedulers config is not a mapping")
        scheduler_config = schedulers[scheduler]
        if not isinstance(scheduler_config, MappingABC):
            raise TypeError("qtop scheduler config is not a mapping")
    except (KeyError, TypeError):
        raise DiffInputError("Scheduler '%s' is not configured in qtopconf.yaml" % scheduler)

    filenames: Dict[str, str] = {}
    for file_key, path_command in scheduler_config.items():
        if not isinstance(path_command, str):
            raise DiffInputError("Scheduler file entry '%s' must be a string command template" % file_key)
        path_template = path_command.strip().split(",", 1)[0].strip()
        try:
            filenames[file_key] = path_template % {"savepath": str(source_dir), "pid": ""}
        except (KeyError, TypeError, ValueError):
            raise DiffInputError("Invalid scheduler path template for '%s': %s" % (file_key, path_template))

    missing = [filename for filename in filenames.values() if not Path(filename).is_file()]
    if missing:
        raise DiffInputError(
            "Missing scheduler input file(s) for %s: %s" % (source_dir, ", ".join(sorted(missing)))
        )
    return filenames


def build_cluster_snapshot(
    name: str,
    scheduler: str,
    source_dir: Path,
    config: Mapping[str, Any],
) -> ClusterSnapshot:
    if scheduler not in BATCH_SYSTEMS:
        raise DiffInputError(
            "Unsupported scheduler '%s'. Available schedulers: %s" % (scheduler, ", ".join(BATCH_SYSTEMS.keys()))
        )

    source_dir = source_dir.resolve()
    files = scheduler_files_for_source(source_dir, scheduler, config)
    options = DiffOptions(SOURCEDIR=str(source_dir))
    batch_system = BATCH_SYSTEMS[scheduler](files, config, options)

    try:
        job_ids, users, states, queues = batch_system.get_jobs_info()
        total_running, total_queued, queue_rows = batch_system.get_queues_info()
        worker_nodes = batch_system.get_worker_nodes(job_ids, queues, options)
    except (KeyError, AttributeError, TypeError, ValueError) as exc:
        raise DiffInputError("Failed to extract scheduler data for '%s' from %s: %s" % (scheduler, source_dir, exc)) from exc

    return ClusterSnapshot(
        name=name,
        scheduler=scheduler,
        source=str(source_dir),
        jobs=_build_jobs(job_ids, users, states, queues),
        queues=_build_queues(queue_rows),
        nodes=_build_nodes(worker_nodes),
        reported_running_jobs=_to_int(total_running),
        reported_queued_jobs=_to_int(total_queued),
    )


def anonymize_snapshot(snapshot: ClusterSnapshot, salt: str = "") -> ClusterSnapshot:
    jobs = OrderedDict(
        (
            _stable_hash_id("job", key, salt),
            JobRecord(
                job_id=_stable_hash_id("job", job.job_id, salt),
                user=_stable_hash_id("user", job.user, salt),
                state=job.state,
                queue=_stable_hash_id("queue", job.queue, salt),
            ),
        )
        for key, job in snapshot.jobs.items()
    )
    queues = OrderedDict()
    for queue in snapshot.queues.values():
        name = _stable_hash_id("queue", queue.name, salt)
        queues[name] = QueueRecord(
            name=name,
            running=queue.running,
            queued=queue.queued,
            limit=_stable_hash_id("queue-limit", queue.limit, salt),
            state=queue.state,
        )
    nodes = OrderedDict(
        (
            _stable_hash_id("node", key, salt),
            NodeRecord(
                name=_stable_hash_id("node", node.name, salt),
                state=node.state,
                cores=node.cores,
                busy_cores=node.busy_cores,
                queues=tuple(sorted(_stable_hash_id("queue", queue, salt) for queue in node.queues)),
                jobs=tuple(sorted(_stable_hash_id("job", job_id, salt) for job_id in node.jobs)),
            ),
        )
        for key, node in snapshot.nodes.items()
    )
    return ClusterSnapshot(
        name=snapshot.name,
        scheduler=snapshot.scheduler,
        source=_stable_hash_id("source", snapshot.source, salt),
        jobs=jobs,
        queues=queues,
        nodes=nodes,
        reported_running_jobs=snapshot.reported_running_jobs,
        reported_queued_jobs=snapshot.reported_queued_jobs,
    )


def compare_snapshots(left: ClusterSnapshot, right: ClusterSnapshot) -> DiffReport:
    return DiffReport(
        left=left,
        right=right,
        metrics=_compare_metrics(left.metrics(), right.metrics()),
        queues=_compare_mapping(left.queues, right.queues, _describe_queue),
        nodes=_compare_mapping(left.nodes, right.nodes, _describe_node),
        jobs=_compare_mapping(left.jobs, right.jobs, _describe_job),
    )


def render_report(
    report: DiffReport,
    show_equal: bool = False,
    color: bool = False,
    limit: int = 80,
    cell_width: int = 48,
) -> str:
    left_name = _safe_display(report.left.name)
    right_name = _safe_display(report.right.name)
    scheduler = _safe_display(report.left.scheduler)
    left_source = _safe_display(report.left.source)
    right_source = _safe_display(report.right.source)
    lines = [
        "qtop differential report",
        "scheduler: %s" % scheduler,
        "left:  %s (%s)" % (left_name, left_source),
        "right: %s (%s)" % (right_name, right_source),
        "",
    ]
    lines.extend(_render_section("Summary", report.metrics, left_name, right_name, True, color, 0, cell_width))
    lines.extend(_render_section("Queues", report.queues, left_name, right_name, show_equal, color, limit, cell_width))
    lines.extend(_render_section("Nodes", report.nodes, left_name, right_name, show_equal, color, limit, cell_width))
    lines.extend(_render_section("Jobs", report.jobs, left_name, right_name, show_equal, color, limit, cell_width))
    return "\n".join(lines)


def write_json_report(report: DiffReport, output_path: Path) -> None:
    payload = {
        "has_differences": report.has_differences,
        "left": _snapshot_to_dict(report.left),
        "right": _snapshot_to_dict(report.right),
        "diff": {
            "metrics": [asdict(row) for row in report.metrics],
            "queues": [asdict(row) for row in report.queues],
            "nodes": [asdict(row) for row in report.nodes],
            "jobs": [asdict(row) for row in report.jobs],
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False)
        handle.write("\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qtop-diff",
        description="Compare two qtop scheduler source directories for first-step differential debugging.",
    )
    parser.add_argument("-b", "--batch-system", "--scheduler", dest="scheduler", required=True, type=str.lower, choices=list(BATCH_SYSTEMS.keys()))
    parser.add_argument("--left-source", required=True, type=Path, help="Source directory for the first cluster, using qtop -s filenames.")
    parser.add_argument("--right-source", required=True, type=Path, help="Source directory for the second cluster, using qtop -s filenames.")
    parser.add_argument("--left-name", default="left", help="Display name for the first cluster.")
    parser.add_argument("--right-name", default="right", help="Display name for the second cluster.")
    parser.add_argument("-f", "--config", type=Path, help="Optional qtopconf.yaml overlay.")
    parser.add_argument("--anonymize", action="store_true", help="Hash sensitive scheduler identifiers deterministically after raw extraction.")
    parser.add_argument("--show-equal", action="store_true", help="Include equal queue/node/job rows in detail sections.")
    parser.add_argument("--limit", type=int, default=80, help="Maximum changed rows per detail section. Use 0 for no limit.")
    parser.add_argument("--cell-width", type=int, default=48, help="Maximum width of each table value cell.")
    parser.add_argument("--color", choices=("auto", "always", "never"), default="auto", help="Colorize changed rows.")
    parser.add_argument("--json-output", type=Path, help="Write machine-readable diff output to this JSON file.")
    parser.add_argument("--fail-on-diff", action="store_true", help="Exit with status 2 when differences are found.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    if args.cell_width < 4:
        parser.error("--cell-width must be >= 4")

    try:
        config = load_config(args.config)
        scheduler = args.scheduler
        left = build_cluster_snapshot(args.left_name, scheduler, args.left_source, config)
        right = build_cluster_snapshot(args.right_name, scheduler, args.right_source, config)
        if args.anonymize:
            salt = os.environ.get("QTOP_ANONYMIZE_SALT", "")
            left = anonymize_snapshot(left, salt)
            right = anonymize_snapshot(right, salt)
        report = compare_snapshots(left, right)
        color = _should_color(args.color)
        print(render_report(report, args.show_equal, color, args.limit, args.cell_width))
        if args.json_output is not None:
            write_json_report(report, args.json_output)
    except DiffInputError as exc:
        sys.stderr.write("%s\n" % exc)
        return 1
    except OSError as exc:
        sys.stderr.write("%s\n" % exc)
        return 1

    return 2 if args.fail_on_diff and report.has_differences else 0


def _build_jobs(job_ids: Sequence[Any], users: Sequence[Any], states: Sequence[Any], queues: Sequence[Any]) -> Mapping[str, JobRecord]:
    lengths = {
        "job_ids": len(job_ids),
        "users": len(users),
        "states": len(states),
        "queues": len(queues),
    }
    if len(set(lengths.values())) != 1:
        raise DiffInputError("mismatched job vector lengths: %s" % ", ".join("%s=%s" % item for item in lengths.items()))

    jobs: Dict[str, JobRecord] = OrderedDict()
    seen: Counter = Counter()
    for job_id, user, state, queue in zip(job_ids, users, states, queues):
        normalized_job_id = _stringify(job_id)
        seen[normalized_job_id] += 1
        key = normalized_job_id if seen[normalized_job_id] == 1 else "%s#%s" % (normalized_job_id, seen[normalized_job_id])
        jobs[key] = JobRecord(
            job_id=normalized_job_id,
            user=_stringify(user),
            state=_stringify(state),
            queue=_stringify(queue),
        )
    return jobs


def _build_queues(queue_rows: Iterable[Mapping[str, Any]]) -> Mapping[str, QueueRecord]:
    queues: Dict[str, QueueRecord] = OrderedDict()
    for queue in queue_rows:
        if not isinstance(queue, MappingABC):
            raise DiffInputError("queue entry must be a mapping")
        name = _stringify(queue.get("queue_name", ""))
        if not name:
            continue
        queues[name] = QueueRecord(
            name=name,
            running=_to_int(queue.get("run", 0)),
            queued=_to_int(queue.get("queued", 0)),
            limit=_stringify(queue.get("lm", "")),
            state=_stringify(queue.get("state", "")),
        )
    return queues


def _build_nodes(worker_nodes: Iterable[Mapping[str, Any]]) -> Mapping[str, NodeRecord]:
    nodes: Dict[str, NodeRecord] = OrderedDict()
    for node in worker_nodes:
        if not isinstance(node, MappingABC):
            raise DiffInputError("worker node entry must be a mapping")
        name = _stringify(node.get("domainname", ""))
        if not name:
            continue
        core_job_map = node.get("core_job_map", {}) or {}
        if not isinstance(core_job_map, MappingABC):
            raise DiffInputError("node '%s' core_job_map must be a mapping" % name)
        raw_queues = node.get("qname", [])
        if isinstance(raw_queues, str):
            raw_queues = [raw_queues]
        elif raw_queues is None:
            raw_queues = []
        elif not isinstance(raw_queues, IterableABC):
            raise DiffInputError("node '%s' qname must be a string or iterable" % name)
        queues = tuple(sorted(_stringify(queue) for queue in raw_queues if queue is not None))
        jobs = tuple(sorted(set(_stringify(job_id) for job_id in core_job_map.values() if job_id is not None)))
        nodes[name] = NodeRecord(
            name=name,
            state=_stringify(node.get("state", "")),
            cores=_to_int(node.get("np", 0)),
            busy_cores=sum(1 for job_id in core_job_map.values() if job_id is not None),
            queues=queues,
            jobs=jobs,
        )
    return nodes


def _compare_metrics(left: Mapping[str, int], right: Mapping[str, int]) -> Tuple[ComparisonRow, ...]:
    rows: List[ComparisonRow] = []
    for key in sorted(set(left) | set(right)):
        left_value = left.get(key, 0)
        right_value = right.get(key, 0)
        delta = right_value - left_value
        right_text = "%s (%+d)" % (right_value, delta) if delta else str(right_value)
        rows.append(ComparisonRow(key, str(left_value), right_text, "same" if left_value == right_value else "changed"))
    return tuple(rows)


def _compare_mapping(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    describe: Any,
) -> Tuple[ComparisonRow, ...]:
    rows: List[ComparisonRow] = []
    for key in sorted(set(left) | set(right), key=_natural_key):
        left_value = left.get(key)
        right_value = right.get(key)
        if left_value is None:
            rows.append(ComparisonRow(key, "", describe(right_value), "only_right"))
        elif right_value is None:
            rows.append(ComparisonRow(key, describe(left_value), "", "only_left"))
        else:
            left_text = describe(left_value)
            right_text = describe(right_value)
            rows.append(ComparisonRow(key, left_text, right_text, "same" if left_text == right_text else "changed"))
    return tuple(rows)


def _describe_job(job: JobRecord) -> str:
    return "user=%s state=%s queue=%s" % (job.user, job.state, job.queue)


def _describe_queue(queue: QueueRecord) -> str:
    return "run=%s queued=%s state=%s lm=%s" % (queue.running, queue.queued, queue.state, queue.limit)


def _describe_node(node: NodeRecord) -> str:
    queues = ",".join(node.queues) if node.queues else "-"
    jobs = ",".join(node.jobs) if node.jobs else "-"
    return "state=%s cores=%s busy=%s queues=%s jobs=%s" % (node.state, node.cores, node.busy_cores, queues, jobs)


def _render_section(
    title: str,
    rows: Sequence[ComparisonRow],
    left_name: str,
    right_name: str,
    show_equal: bool,
    color: bool,
    limit: int,
    cell_width: int,
) -> List[str]:
    visible_rows = list(rows if show_equal else [row for row in rows if row.status != "same"])
    hidden = 0
    if limit > 0 and len(visible_rows) > limit:
        hidden = len(visible_rows) - limit
        visible_rows = visible_rows[:limit]

    if not visible_rows:
        return [title, "  no differences", ""]

    table_rows = [
        (
            _status_marker(row.status),
            row.key,
            row.left,
            row.right,
        )
        for row in visible_rows
    ]
    lines = [title]
    lines.extend(_format_table(("diff", "key", left_name, right_name), table_rows, color, cell_width))
    if hidden:
        lines.append("  ... %s more changed row(s) hidden by --limit" % hidden)
    lines.append("")
    return lines


def _format_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    color: bool,
    cell_width: int,
) -> List[str]:
    width = min(max(shutil.get_terminal_size((140, 24)).columns, 80), 180)
    max_value_width = max(12, min(cell_width, max(16, (width - 24) // 2)))
    widths = [len(header) for header in headers]
    max_widths = [6, 28, max_value_width, max_value_width]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = min(max(widths[idx], len(_safe_display(value))), max_widths[idx])

    def format_row(row: Sequence[str], status: str = "same") -> str:
        cells = [_clip(_safe_display(value), widths[idx]).ljust(widths[idx]) for idx, value in enumerate(row)]
        text = "  " + "  ".join(cells).rstrip()
        return _colorize(status, text, color)

    lines = [format_row(headers), "  " + "  ".join("-" * width for width in widths)]
    for row in rows:
        lines.append(format_row(row, _status_from_marker(row[0])))
    return lines


def _status_marker(status: str) -> str:
    return {"same": "=", "changed": "!=", "only_left": "<", "only_right": ">"}.get(status, "?")


def _status_from_marker(marker: str) -> str:
    return {"=": "same", "!=": "changed", "<": "only_left", ">": "only_right"}.get(marker, "changed")


def _colorize(status: str, text: str, color: bool) -> str:
    if not color or status == "same":
        return text
    color_code = {
        "changed": Ansi.yellow,
        "only_left": Ansi.red,
        "only_right": Ansi.green,
    }.get(status, Ansi.dim)
    return "%s%s%s" % (color_code, text, Ansi.reset)


def _clip(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _safe_display(value: str) -> str:
    value = ANSI_ESCAPE_RE.sub("", value)
    return "".join(ch if ch == "\t" or (ch.isprintable() and ch != "\x1b") else "?" for ch in value)


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "str"):
        return str(value.str() if callable(value.str) else value.str)
    return str(value)


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _natural_key(value: str) -> Tuple[Tuple[int, Any, str], ...]:
    parts: List[Tuple[int, Any, str]] = []
    for part in re.split(r"(\d+)", value):
        if not part:
            continue
        if part.isdigit():
            parts.append((0, int(part), part))
        else:
            parts.append((1, part.casefold(), part))
    return tuple(parts)


def _stable_hash_id(namespace: str, value: str, salt: str = "") -> str:
    if not value:
        return value
    digest = hashlib.sha256(f"{salt}\0{namespace}\0{value}".encode("utf-8")).hexdigest()[:20]
    return "%s_%s" % (namespace, digest)


def _snapshot_to_dict(snapshot: ClusterSnapshot) -> Mapping[str, Any]:
    return {
        "name": snapshot.name,
        "scheduler": snapshot.scheduler,
        "source": snapshot.source,
        "metrics": dict(snapshot.metrics()),
        "jobs": {key: asdict(value) for key, value in snapshot.jobs.items()},
        "queues": {key: asdict(value) for key, value in snapshot.queues.items()},
        "nodes": {key: asdict(value) for key, value in snapshot.nodes.items()},
    }


def _should_color(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


if __name__ == "__main__":
    sys.exit(main())
