# Prerequisites

| Requirement | Applies to | What to verify |
|---|---|---|
| Docker Engine and Compose plugin | Containers | The CLI can access the intended daemon. Run `docker version` and `docker compose version`. |
| Linux or WSL2 Linux workspace | Host utilities | Docker integration is enabled for the selected distribution. Run from its Linux filesystem. |
| Python 3.11 or 3.12 and Make | Host tools and unit checks | Create a virtual environment. The Airflow and Spark images have separate runtime dependencies. |
| Registry and Maven access or cached artifacts | First build and first Spark runs | The host can pull base images. The Spark JVM can resolve the pinned JDBC and Kafka artifacts. |
| External SQL Server access | Full batch and CDC | Confirm source DNS, port, TLS trust and read permissions from the containers. SQL Server is outside Compose. |
| DBA-verified readiness | SQL Server paths | Enable snapshot isolation for batch. For CDC, check capture instances, Agent/capture/cleanup jobs and retention. |

A host can run unit tests and read the project without source access. For a constrained host, start the required profile and run a large replay or backfill separately. The checked-in service budgets and observed memory-pressure incident are explained in [Resource management](../03-infrastructure/resource-management.md).

[Installation](installation.md) · [SQL Server source](../04-data-ingestion/sql-server-source.md) · [CDC prerequisites](../05-streaming-and-cdc/sql-server-cdc.md)
