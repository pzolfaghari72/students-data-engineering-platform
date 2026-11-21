# Services and Networking

All Compose services join the `platform` user-defined bridge network. A
container resolves peer services by their Compose service name; host port
mapping is a separate interface intended for local tools.

| Client | Internal destination | Host-published port |
| --- | --- | --- |
| Airflow/Spark → PostgreSQL | `postgres` plus configured container port | `15433` |
| Spark/Grafana → ClickHouse | `clickhouse` plus configured HTTP/native ports | `18123` (HTTP), `19000` (native) |
| Debezium/Spark → Kafka | `kafka` plus internal broker port | `19094` |
| Registration/operator → Connect REST | `connect` plus internal REST port | `18083` |
| Browser → Airflow/Grafana | Host loopback | `8080` (Airflow), `13000` (Grafana) |

Host-facing mappings are bound to `127.0.0.1`; this is suitable for local
development, not remote access. Change these mappings in `docker-compose.yml`.
Never use a host-published address for container to
container traffic when the Compose service DNS endpoint is available.

External SQL Server reachability depends on host routing, firewall rules and
the configured source address; Compose provides `host.docker.internal` as an
extra host for selected services but does not guarantee that the source is
reachable.
