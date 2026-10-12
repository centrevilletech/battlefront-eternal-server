# Bounded input acknowledgement recovery

This isolated change addresses a recoverable parent scheduling pause. It does not improve simulation throughput or remote entity freshness, and it does not change the publication-worker watchdog.

The authority keeps exactly one sent ACK batch and one replaceable latest batch. Each batch carries a lane incarnation, monotonic batch ID and the original authority capture timestamp. The pending batch is the latest complete per-player ACK and cumulative unacknowledged edge feedback; it is not a delta. No state payload is resent during recovery.

- Normal: a batch has 2 seconds to receive parent-processing credit. Credit remains independent of client ACKs and publication-worker results.
- Deadline: yield one poll/check turn so queued credit can run first. If credit is still absent, mark the lane stalled and send one bounded liveness probe for that incarnation and batch. Retain the same two slots.
- Parent recovery: after synchronous consumers finish, the parent records the last processed batch and returns exact credit. A probe can replay that credit only when both incarnation and batch match the recorded completion. A mismatched response cannot resume the lane.
- Stale output: parent consumers skip native ACK/edge-feedback sends older than 250 ms, but still complete processing credit. The latest pending batch can then replace the old output. Original capture timestamps are never refreshed during dispatch or recovery.
- Resume: matching credit clears the stalled state and dispatches only the latest pending batch. Edge outcomes stay cumulative until their separate client ACK, so skipping old transport output does not retire them.
- Terminal: if the exact inflight batch still has no valid credit 10 seconds after its original dispatch, fail the lane and match observably through the existing fatal path. The terminal deadline is finite and cannot be renewed by offers, probes, stale credits or wrong incarnations. A completely dead parent also ends its workers when the process exits; this deadline covers a live but broken credit path.

The 10-second terminal bound is an explicit recovery policy, not a capacity claim. It allows the measured roughly 3-second host pause to recover without immediately destroying a healthy match while keeping queued memory constant. Stalled/resumed state is exposed in health/metrics and logged once per transition. It does not permit stale state delivery or an expanding retry queue.

Tests must cover injected 3-second pause, stale output skipped with exact credit, latest cumulative edge feedback surviving replacement, wrong incarnation/batch, one probe maximum, fixed terminal deadline, terminal non-resurrection, and credit bound to the old match across rotation. Native send and complete server suites remain required before any promotion.
