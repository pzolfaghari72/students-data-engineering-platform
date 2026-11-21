# ADR-004: Docker Compose Deployment

## Status

Accepted for the local learning environment.

## Context

The platform must be reproducible on a developer workstation with limited
memory and a manageable number of technologies.

## Decision

Use a single Docker Compose project with one bridge network, named volumes and
profiles for batch, CDC, streaming, business dashboards and one-shot tools.

## Alternatives considered

- Kubernetes or multi-node orchestration: excess setup/resource complexity
  for the intended local lab.
- Start every service by default: unnecessary memory use and risky consumer
  ownership.

## Consequences

Profiles allow selective startup, but the service dependency graph still
controls what Compose starts. Single-node Kafka replication factor one and
local database volumes are not highly available.

## Implementation references

`docker-compose.yml`, `Makefile`, and
[Compose service inventory](../03-infrastructure/docker-compose.md).
