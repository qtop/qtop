# qtop architecture

qtop collects scheduler output, turns it into a common cluster state, and
presents jobs, queues, nodes and core occupancy in a terminal. This overview
adapts the GitDiagram description proposed in
[issue #571](https://github.com/qtop/qtop/issues/571) to the current source.
The diagram is stored as Mermaid so it can be reviewed and updated with the
code, and rendered directly on GitHub.

## Runtime overview

Solid arrows show control or data flow; dotted arrows show optional outputs.
Several stages live in `qtop_py/qtop.py`; the groups describe responsibilities
within the existing program.

```mermaid
flowchart TB
    operator([Operator])

    subgraph entry["Entry and control"]
        cli["CLI entry points: qtop / cli.py"]
        runtime["Runtime and watch loop: qtop.py"]
        options["Options and logging: utils.py"]
        config["Configuration: qtopconf.yaml / yaml_parser.py"]
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
        document["Document: qtop.py"]
        calculations["Cluster, WNOccupancy and core mapping: qtop.py"]
        model --> document
        document --> calculations
    end
    shared --> model

    subgraph outputs["Views and outputs"]
        render["TextDisplay: qtop.py"]
        terminal["Terminal dashboard"]
        viewport["Navigation: ui/viewport.py"]
        colors["Color mappings: colormap.py"]
        json["JSON export: Document.save in qtop.py"]
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
    document -.-> json
    runtime -.-> capture
    runtime -.-> replay
```

## Following a snapshot through the code

1. The CLI enters `cli_main()` and `main()` in
   [qtop.py](../qtop_py/qtop.py). The runtime loads configuration, selects a
   registered backend, and either executes its configured commands or reads
   files supplied with `-s`. The demo backend generates fictional data.
2. Each backend implements `get_jobs_info()`, `get_queues_info()` and
   `get_worker_nodes()`. `GenericBatchSystem.get_cluster_state()` in
   [serialiser.py](../qtop_py/serialiser.py) combines their results through
   [build_cluster_state()](../qtop_py/cluster_state.py).
3. `Document.from_cluster_state()` adapts that plain-data snapshot for the
   existing runtime. `Cluster`, `WNFilter` and `WNOccupancy` calculate node
   layout, filtering, user accounting and per-core occupancy. `TextDisplay`
   renders the three terminal sections, using viewport navigation and color
   mappings.
4. `-E` exports the snapshot as JSON. `-L` creates a support sample archive.
   `-R` replays saved rendered frames. The experimental `-B` web interface
   serves exported JSON and requires `-e`; these are distinct output paths.

## Source map

| Responsibility | Source |
| --- | --- |
| Launchers and installed entry point | [qtop](../qtop), [cli.py](../qtop_py/cli.py), [pyproject.toml](../pyproject.toml) |
| Runtime orchestration, calculations and terminal rendering | [qtop.py](../qtop_py/qtop.py) |
| Command-line options and logging | [utils.py](../qtop_py/utils.py) |
| Configuration defaults and parsing | [qtopconf.yaml](../qtopconf.yaml), [yaml_parser.py](../qtop_py/yaml_parser.py) |
| Scheduler backends | [Slurm](../qtop_py/plugins/slurm.py), [PBS](../qtop_py/plugins/pbs.py), [SGE](../qtop_py/plugins/sge.py), [OAR](../qtop_py/plugins/oar.py), [demo](../qtop_py/plugins/demo.py) |
| Shared extraction and backend interface | [serialiser.py](../qtop_py/serialiser.py) |
| Common snapshot construction, validation and totals | [cluster_state.py](../qtop_py/cluster_state.py) |
| Navigation and color definitions | [viewport.py](../qtop_py/ui/viewport.py), [colormap.py](../qtop_py/colormap.py) |
| Capture archives and temporary files | [fileutils.py](../qtop_py/fileutils.py) |
| Experimental HTTP output | [web.py](../qtop_py/web.py), [web/index.html](../qtop_py/web/index.html) |

## Keeping the overview current

When collection, state or output responsibilities change, update the Mermaid
source and source map in this file. Check new backend coverage through the
[shared sample gates](ci-sample-gates.md), and consult the
[CI structure](ci-structure.md) for validation entry points. User-facing
commands and display behavior are described in the [qtop guide](documentation.md).
