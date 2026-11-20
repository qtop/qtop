# Synthetic HTCondor fixtures

These MIT-licensed traces are authored for qtop, not captured from a pool.
They follow HTCondor's JSON ClassAd output for:

- `condor_status.json`: `condor_status -json -attributes Name,Machine,SlotType,PartitionableSlot,DynamicSlot,State,Activity,Cpus,TotalSlotCpus`
- `condor_q.json`: `condor_q -allusers -json -attributes ClusterId,ProcId,Owner,JobStatus,RemoteHost,RequestCpus,AcctGroup`

The pool has five machines: two partitionable machines with dynamic slots
(one partly, one fully claimed), a drained partitionable machine, a machine
with four static one-core slots (one running, one suspended job) and a
partitionable machine in the Owner state. The eight jobs cover JobStatus
1 (idle), 2 (running), 5 (held) and 7 (suspended), with and without an
accounting group. Partitionable slots report their unclaimed cores in `Cpus`
and the machine total in `TotalSlotCpus`.

Real-pool traces are still needed to confirm site variations such as
multiple schedds (`condor_q -global`), GPUs, and `condor_q` printing nothing
when no jobs exist.
