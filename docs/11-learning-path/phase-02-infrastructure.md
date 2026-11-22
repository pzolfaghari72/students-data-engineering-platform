# Phase 2 — Infrastructure

**Objectives:** understand Compose services, profiles, DNS, ports, bind
mounts, named volumes and optional resource use.

**Read:** [Docker fundamentals](../03-infrastructure/docker-fundamentals.md),
[Compose service inventory](../03-infrastructure/docker-compose.md),
[networking](../03-infrastructure/services-and-networking.md), and
[persistence](../03-infrastructure/volumes-and-persistence.md).

**Inspect:** `docker-compose.yml`, `.env.example`, Makefile and the deployment
diagram. Never read or share `.env` values during this exercise.

**Exercise:** run the core startup/status procedures in the
[operations guide](../10-operations/operational-runbook.md); identify which
services are always available and which are profile-gated.

**Expected result:** distinguish host-published ports from internal DNS and
explain which volumes preserve data across container recreation.

**Validation:** Compose config parses and PostgreSQL/ClickHouse health checks
report healthy. This does not prove a pipeline integration.
