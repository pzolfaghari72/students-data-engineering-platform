# ADR-005: SQL Server Source Connectivity

## Status

Accepted in implementation; source runtime has partial evidence.

## Context

The source is an external SQL Server instance. Connection details differ by
environment and must not be embedded in application code.

## Decision

Use ODBC Driver 18 and `pyodbc` in Airflow for source snapshots, with all
endpoint, driver, TLS and authentication settings required through
environment variables. Debezium receives corresponding settings from the
same configuration contract.

## Alternatives considered

- Hard-coded endpoint/credential defaults: rejected because they silently
  bind the code to a specific environment.
- Local SQL Server container: not part of this real-source project.

## Consequences

The source owner controls reachability, TLS, logins and database options.
Missing settings fail explicitly. Local template values are not runtime
configuration.

## Implementation references

`airflow/plugins/Dockerfile`, `loader.py`,
`debezium/register_connector.py`, and
[Environment Configuration](../01-getting-started/environment-configuration.md).
