// Copyright (c) 2026 qtop contributors
// SPDX-License-Identifier: MIT

// Structural contract only; Python owns QID equality, uniqueness, containment
// cycles, numeric-string bounds and scheduler-specific extension validation.
package schema

#ClusterState: #V1 | #V2 | #V3Beta1
#V1: {
    #Data
    schema_version?: 1
    jobs: [...{qid?: _|_, kind?: _|_, parent_qid?: _|_, ...}]
    nodes: [...{qid?: _|_, ...}]
    queues: [...{qid?: _|_, ...}]
}
#V2: {
    #Data
    schema_version: 2
    source: #Source
    jobs: [...{parent_qid?: _|_, ...}]
}
#V3Beta1: {
    #Data
    schema_version: 3
    schema_revision: "beta1"
    source: #Source
}
#Source: {backend: string & =~"^[^:]+$", cluster_id: string & =~"^[^:]+$"}
#Data: {
    jobs: [...#Job]
    nodes: [...#Node]
    queues: [...#Queue]
    slurm_diagnostics?: {
        rejected_squeue_rows?: int & >=0
        rejected_sinfo_rows?: int & >=0
        jobs_with_missing_nodes?: int & >=0
        jobs_with_invalid_nodelists?: int & >=0
    }
    oar_diagnostics?: {
        missing_resources?: int & >=0
        extra_resources?: int & >=0
        unlisted_jobs?: int & >=0
        shared_resources?: int & >=0
    }
}
#Job: {
    id: string, user: string, state: string, queue: string
    qid?: string
    kind?: "job" | "pod" | "vm" | "task"
    parent_qid?: string
    ...
}
#Node: {
    domainname: string, state: string
    np: (int & >=0 & <=65536) | string
    qname: [...string | null]
    core_job_map: {[string]: string}
    qid?: string
    kind?: _|_
    parent_qid?: _|_
    ...
}
#Queue: {
    name: string, limit: string, state: string
    queued: int & >=0, running: int & >=0
    qid?: string
    kind?: _|_
    parent_qid?: _|_
    ...
}
