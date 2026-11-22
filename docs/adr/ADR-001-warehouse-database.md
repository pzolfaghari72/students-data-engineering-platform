# ADR-001 — One ClickHouse server with separate analytical databases

**Status:** Accepted

Use one ClickHouse server for a batch dimensional warehouse and a dedicated CDC database. Batch publishes reconciled full snapshots. CDC verifies offset coverage and serves current state plus deduplicated logical events. Each database has its own writer. Grafana reads approved business and health views through restricted privileges.

Separate databases allow independent recovery, retention and access. Plain MergeTree with publication metadata supports the volume and local deployment without a second analytics service. Revisit physical optimizations when measured query cost warrants them.
