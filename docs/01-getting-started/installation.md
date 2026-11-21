# Installation

Installation prepares local configuration and validates Compose; it does not
load source data or register a Debezium connector.

## Sequence

1. Clone the repository and work from its root.
2. Copy `.env.example` to `.env`, then edit the source endpoint and credentials
   supplied by the SQL Server owner. Never commit or share `.env`.
3. Install host-side Python requirements if running validation utilities
   outside containers.
4. Run `make init`; it preserves existing `.env`, generates missing secrets,
   checks local tools, creates required directories, and validates Compose.
5. Use the [quick start](./quick-start.md) to start only the required profile.

The full environment variable contract is in
[Environment Configuration](./environment-configuration.md). `.env.example`
is documentation/template input only; services use `.env` or injected
environment variables.

## Image builds and first use

Airflow, Spark, Debezium Connect and Grafana use repository Dockerfiles.
External image and package downloads are required. The first Spark execution
can resolve JDBC artifacts from Maven; no warehouse Spark job is run by
installation.

## Safety checks

- Confirm the target SQL Server host/database before providing source access.
- Use a read-only source login for the batch extract; provision a dedicated
  CDC reader for Debezium.
- Prefer certificate validation for SQL Server TLS.
- Do not use `docker compose down -v` as routine cleanup; it removes persistent
  data. Persistence behavior is documented in
  [Volumes and Persistence](../03-infrastructure/volumes-and-persistence.md).
