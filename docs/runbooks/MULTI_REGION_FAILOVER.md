# TaskFlow multi-region failover runbook

## Topology

- Region A is the normal writer region.
- Region B maintains a GTID MySQL replica and a warm TaskFlow deployment.
- The global traffic layer prefers Region A and uses Region B only after the failover decision.
- Object/archive evidence remains replicated independently of the SQL promotion path.

## Failover decision

1. Confirm Region A is unavailable rather than merely degraded.
2. Freeze or fence Region A writers before promoting Region B. A network partition must not create two SQL writers.
3. Record the last known Region A GTID set and compare it with Region B. If Region B has not applied the required transaction set, declare non-zero RPO explicitly instead of hiding it.
4. Stop replication in Region B and enable writes only after the old writer is fenced.
5. Repoint the Region B TaskFlow deployment to the promoted local writer.
6. Move global traffic to Region B and verify `/health/ready`, an existing task read, and a new durable write.

## Failback

Failback is a new migration, not a reverse toggle. Rebuild or resynchronize Region A from the promoted Region B source, verify GTID continuity, restore replication, then perform a controlled writer switchover. Never attach an old Region A primary as a writer without reconciliation.

## Exit criteria

- Region A writer is fenced;
- Region B contains the pre-failure durable marker (RPO evidence);
- Region B accepts a new post-promotion write;
- global traffic returns success through the same endpoint;
- measured RTO/RPO evidence is retained;
- audit, governance and archive evidence remain available;
- a separate failback plan has been reviewed before restoring Region A traffic.
