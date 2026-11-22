# ADR-004 — Single-host Compose deployment

**Status:** Accepted

Use Compose, a bridge network, loopback host bindings and named volumes. Airflow LocalExecutor launches the Spark batch job. A separately managed Spark service owns the CDC query. Kafka and Connect start with the CDC profile, and the stream starts with the streaming profile. Load deployment values from the ignored `.env`; use the template to document the contract.

This keeps the platform reproducible on a developer machine. Each local volume has a compatible backup and restore boundary described in the operations guide. The deployment has a single-host failure domain.
