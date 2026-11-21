# Docker Deployment Diagram

This is the canonical Compose service map. Only the two Spark services have
profiles; the Make targets select service names to avoid starting every
unprofiled service.

```mermaid
flowchart TB
    subgraph Host["Developer host / Docker"]
        subgraph Default["Unprofiled services (included by raw compose up)"]
            PG[("postgres<br/>postgres-data")]
            CH[("clickhouse<br/>clickhouse-data")]
            Init["postgres-init / staging-schema-init / clickhouse-init"]
            AF["Airflow API / scheduler / DAG processor"]
            K[("Kafka<br/>kafka-data")]
            C["Kafka Connect + JMX snapshot-completion signal"]
            Reg["debezium-register<br/>one-shot"]
            G["Grafana KPI dashboards<br/>grafana-data"]
        end
        subgraph Batch["batch profile"]
            SB["spark-warehouse<br/>one-shot"]
        end
        subgraph Streaming["streaming profile"]
            SS["spark-cdc-stream"]
            CP[("spark-streaming-checkpoints")]
        end
        PG --> AF
        CH --> AF
        PG --> SB
        CH --> SB
        K --> C
        C --> Reg
        Reg --> SS
        K --> SS
        PG --> SS
        CH --> SS
        SS --> CP
        G --> CH
    end
    SQL["External SQL Server"] --> AF
    SQL --> C
    Browser["Host browser"] --> G
```

All inter-container traffic uses the `platform` bridge network and Compose
service DNS. Host-published ports bind to loopback. An unqualified
`docker compose up` includes the unprofiled Kafka, Connect, and registration
services. Prefer the explicit Make targets for normal startup; do not start
all services or profiles unless CDC prerequisites and connector registration
are intentionally being handled.
