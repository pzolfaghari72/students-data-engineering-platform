# Docker deployment diagram

```mermaid
flowchart TB
    U["Host browser and clients"] -->|Loopback bindings| AF["Airflow API and UI"]
    U -->|Loopback bindings| G["Grafana"]
    ES[("External SQL Server")] --> AS["Airflow scheduler / LocalExecutor"]
    ES --> D["Connect and Debezium"]
    subgraph DC["One Docker Compose project and platform bridge"]
      AF --> AP["PostgreSQL Airflow metadata"]
      AS --> AP
      DP["Airflow DAG processor"] --> AP
      AS --> PG["PostgreSQL raw staging"]
      AS --> B["Spark batch process"]
      PG --> B --> CH["ClickHouse Batch database"]
      subgraph CDC["CDC profile services"]
        D --> K["Kafka KRaft broker"]
      end
      K --> SS["Spark CDC worker in streaming profile"]
      SS --> CP[("Spark checkpoint volume")]
      SS --> CD["ClickHouse CDC database"]
      CH --> G
      CD --> G
    end
```

The two ClickHouse databases share a server. PostgreSQL hosts both of its listed databases. Compose persists database files, broker data, checkpoints and Grafana state in named volumes. One-shot jobs initialize roles, schemas and connector registration. [Service inventory](../../03-infrastructure/docker-compose.md) · [Ports](../../03-infrastructure/services-and-networking.md)
