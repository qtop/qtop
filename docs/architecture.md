# qtop architecture

qtop collects scheduler output, turns it into a common cluster state, and
presents jobs, queues, nodes and core occupancy in a terminal. This overview
adapts the GitDiagram description proposed in
[issue #571](https://github.com/qtop/qtop/issues/571) to the current source.
The diagram is stored as Mermaid so it can be reviewed and updated with the
code, and rendered directly on GitHub.
The data contract between backends and views, its schema versions and its
logical relations are described in [cluster-state-model.md](cluster-state-model.md).

## Runtime overview

Solid arrows show control or data flow; dotted arrows show optional outputs.
`qtop_py/qtop.py` owns runtime I/O and effect execution. The MVU modules
separate state transitions, snapshot queries and terminal presentation.

```mermaid
flowchart TB
    operator([Operator])

    subgraph entry["Entry and control"]
        cli["CLI entry points: qtop / qtop_py.qtop"]
        runtime["Refresh stages and watch loop: qtop.py"]
        update["MVU_Model / MVU_update: mvu.py"]
        runtime --> update
        update -->|effect requests| runtime
        options["Options and logging: utils.py"]
        config["Configuration: configuration.py / yaml_parser.py"]
        cli --> runtime
        runtime --> options
        runtime --> config
    end
    operator --> cli

    subgraph collection["Scheduler collection"]
        files["Live scheduler commands or saved traces"]
        slurm["Slurm: plugins/slurm.py"]
        pbs["PBS: plugins/pbs.py"]
        sge["SGE: plugins/sge.py"]
        oar["OAR: plugins/oar.py"]
        demo["Generated demo: plugins/demo.py"]
        shared["Shared extraction and collection: serialiser.py"]
        files --> slurm
        files --> pbs
        files --> sge
        files --> oar
        slurm --> shared
        pbs --> shared
        sge --> shared
        oar --> shared
        demo --> shared
    end
    runtime --> files
    runtime --> demo

    subgraph state["Cluster state and calculations"]
        model["Common jobs, nodes and queues: cluster_state.py"]
        snapshot["Snapshot: snapshot.py"]
        calculations["MVU_query: mvu_query.py"]
        model --> snapshot
        snapshot --> calculations
    end
    shared --> model

    subgraph outputs["Views and outputs"]
        render["MVU_view / TextDisplay: ui/mvu_view.py"]
        terminal["Terminal dashboard"]
        viewport["Navigation: ui/viewport.py"]
        colors["Color mappings: colormap.py"]
        json["JSON export: snapshot.py"]
        capture["Sample archives: fileutils.py"]
        replay["Saved-frame replay: qtop.py"]
        web["Experimental web interface: web.py"]
        render --> terminal
        render --> viewport
        render --> colors
        json -.-> web
        replay --> terminal
    end
    calculations --> render
    snapshot -.-> json
    runtime -.-> capture
    runtime -.-> replay
```

## Following a snapshot through the code

1. The CLI enters `cli_main()` and `main()` in
   [qtop.py](../qtop_py/qtop.py). The runtime loads configuration, selects a
   registered backend, and either executes its configured commands or reads
   files supplied with `-s`. The demo backend generates fictional data.
   The loop calls `MVU_collect_snapshot()`, `MVU_export_snapshot()`,
   `MVU_query()` and `MVU_view()`. `MVU_update()` accepts snapshots and keyboard
   messages, returning a new model and effect requests; the runtime executes
   menu input, user-detail lookups and terminal effects.
   [configuration.py](../qtop_py/configuration.py) owns file precedence,
   normalization, CLI overrides and validation. The runtime passes configuration,
   options and the selected backend explicitly to collection helpers.
2. Each backend implements `get_jobs_info()`, `get_queues_info()` and
   `get_worker_nodes()`. `GenericBatchSystem.get_cluster_state()` in
   [serialiser.py](../qtop_py/serialiser.py) combines their results through
   [build_cluster_state()](../qtop_py/cluster_state.py).
   Backend class hooks also define command adjustments, optional trace aliases,
   detection priority, queue blocking and scheduler-specific display notes.
   The core dispatches capabilities; scheduler support rules validate and mask extensions.
3. `Snapshot.from_cluster_state()` in [snapshot.py](../qtop_py/snapshot.py)
   owns the normalized snapshot and exposes cached legacy rendering fields.
   Exports read that authoritative state; the five-field constructor remains
   available for old trace adapters. `MVU_query()` builds private view data
   with copied nodes, options and viewport. The render model carries
   `display_overrides`, `user_details` and `transposed_sections`. `Cluster`, `WNFilter` and
   `WNOccupancy` in [mvu_query.py](../qtop_py/mvu_query.py) calculate layout,
   filtering, user accounting and per-core occupancy. `MVU_view()` delegates
   the three terminal sections to [mvu_view.py](../qtop_py/ui/mvu_view.py).
   Both stages receive an explicit model rather than reading runtime globals;
   derived layout does not modify the canonical snapshot.
4. `-E` exports the snapshot as JSON. `-L` creates a support sample archive.
   `-R` replays saved rendered frames. The experimental `-B` web interface
   serves exported JSON and requires `-e`; these are distinct output paths.

`RunResources` owns redirected output, temporary frames, web shutdown and replay
workspaces. It finalizes each capture before the next refresh can replace its
traces, restores the launch directory, and reports capture paths at exit.
Menu input restores terminal settings even when interrupted.

[mvu.py](../qtop_py/mvu.py) uses ordinary Python 3.6-compatible classes and
functions, with no framework dependency. Collection still occurs once per
refresh; the MVU split adds no scheduler commands. Legacy imports and controller
entry points remain adapters in `qtop.py`; those adapters still support the old
runtime globals, while the new query/view pipeline does not depend on them.

## Source map

| Responsibility | Source |
| --- | --- |
| Launchers and installed entry point | [qtop](../qtop), [qtop.py](../qtop_py/qtop.py), [pyproject.toml](../pyproject.toml) |
| Runtime orchestration, I/O effects and compatibility adapters (deprecated, see below) | [qtop.py](../qtop_py/qtop.py) |
| Explicit model and update messages | [mvu.py](../qtop_py/mvu.py) |
| Snapshot queries, filtering and occupancy calculations | [mvu_query.py](../qtop_py/mvu_query.py) |
| Terminal presentation | [mvu_view.py](../qtop_py/ui/mvu_view.py) |
| Command-line options and logging | [utils.py](../qtop_py/utils.py) |
| Configuration loading, preparation and parsing | [configuration.py](../qtop_py/configuration.py), [qtopconf.yaml](../qtopconf.yaml), [yaml_parser.py](../qtop_py/yaml_parser.py) |
| Scheduler backends and support | [Slurm](../qtop_py/plugins/slurm.py), [PBS](../qtop_py/plugins/pbs.py), [SGE](../qtop_py/plugins/sge.py), [OAR](../qtop_py/plugins/oar.py), [demo](../qtop_py/plugins/demo.py), [support helpers](../qtop_py/schedulers/) |
| Shared extraction and backend interface | [serialiser.py](../qtop_py/serialiser.py) |
| Shared state/configuration types and backend protocol | [state_types.py](../qtop_py/state_types.py) |
| Canonical snapshot and legacy adapter | [snapshot.py](../qtop_py/snapshot.py) |
| Replay time and account expression parsing | [input_parsing.py](../qtop_py/input_parsing.py) |
| Run resource ownership and frame/archive finalization | [runtime.py](../qtop_py/runtime.py) |
| Common snapshot construction, validation and totals | [cluster_state.py](../qtop_py/cluster_state.py) |
| Navigation and color definitions | [controls.py](../qtop_py/ui/controls.py), [viewport.py](../qtop_py/ui/viewport.py), [colormap.py](../qtop_py/colormap.py) |
| Capture archives and temporary files | [fileutils.py](../qtop_py/fileutils.py) |
| Experimental HTTP output | [web.py](../qtop_py/web.py), [web/index.html](../qtop_py/web/index.html) |

### Deprecated legacy adapters

`qtop.py` still exports the pre-MVU entry points `MVU_legacy_model()`, `colorize()`,
`control_qtop()` and the `Cluster`, `WNOccupancy` and `TextDisplay` classes. They build their
model from module globals (`args`, `config`, `dynamic_config`, `viewport` and so on) that
callers must set first. Since 0.9.20261110 they raise a `DeprecationWarning` and they will be
scheduled for removal in 1.0. `main()` and `tools/validate_trace_export.py` use none of them:
build an `MVU_Model` and pass it to `MVU_update`, `MVU_effects`, `MVU_query` and `MVU_view`.
`main()` sets no module globals; `tests/test_runtime.py` fails if a run changes module-level
state or triggers a legacy adapter.

Before 1.0, retain these public adapters and their warning/compatibility tests.
New presentation tests call ui.mvu_view.TextDisplay directly with an explicit
model; existing global-context tests are migrated as their fixtures change.
The 1.0 removal commit must delete the adapters, their global declarations and
adapter-only tests together, update the public import documentation, and pass
pre-pr plus schema-ci. Keep the pure helper re-exports outside this retirement.
This is a compatibility commitment, not permission to remove APIs in 0.9.

## Keeping the overview current

When collection, state or output responsibilities change, update the Mermaid
source and source map in this file. Check new backend coverage through the
[shared sample gates](ci-sample-gates.md), and consult the
[CI structure](ci-structure.md) for validation entry points. User-facing
commands and display behavior are described in the [qtop guide](documentation.md).
