# Visual backlog triage - 2026-05-21

This note groups five old visual-backlog issues so maintainers can close, defer,
or re-scope them without re-reading the full issue history.

| Issue | Recommendation | Rationale |
| --- | --- | --- |
| #7 - admin-customizable colour coding | Defer until color configuration has a documented schema. | The issue asks for user-defined state coloring, but the current colormap path is still global and not exposed as a stable user contract. |
| #10 - states from multiple CPUs per node reporting | Keep open as a visualization enhancement. | The request is still distinct from normal occupancy rows because it asks for per-node vertical state summaries. |
| #11 - report container jobs visible under `oarstat -S` | Re-scope under the OAR plugin before UI work. | The visual output depends on reliable OAR container-job extraction first. |
| #12 - best-effort jobs/queues visualization | Link to #7 and keep as product/design follow-up. | Best-effort coloring overlaps with custom node-state coloring, but has a narrower queue/job semantics requirement. |
| #34 - visualize fat nodes | Keep open, but require a sample fixture before implementation. | The UI behavior is hard to validate without representative multi-core or high-density node input. |

Suggested closeout: keep #10, #12, and #34 as explicit visual backlog items,
move #11 behind OAR data extraction, and treat #7 as the shared configuration
dependency for any future color-policy work.
