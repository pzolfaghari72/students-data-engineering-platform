PYTHON ?= $(shell if test -x .venv/bin/python; then echo .venv/bin/python; else command -v python3; fi)

.PHONY: init install compose-config up up-core up-batch up-pipeline cdc-up streaming-up dashboards-up status logs test staging-init clickhouse-users warehouse-init cdc-warehouse-init warehouse-load down

init:
	bash scripts/initialize_project.sh

install:
	$(PYTHON) -m pip install -r requirements.txt

compose-config:
	docker compose config --quiet

up: up-pipeline

up-core: init
	docker compose up -d --wait postgres clickhouse
	$(MAKE) staging-init
	$(MAKE) clickhouse-users
	$(MAKE) warehouse-init
	$(MAKE) cdc-warehouse-init

up-batch: up-core
	docker compose --profile batch up -d --build airflow-api-server airflow-scheduler airflow-dag-processor

up-pipeline: up-core
	docker compose --profile batch --profile dashboards up -d --build airflow-api-server airflow-scheduler airflow-dag-processor grafana

cdc-up:
	docker compose --profile cdc up -d --build --wait kafka connect
	docker compose --profile cdc --profile tools run --rm debezium-register

streaming-up: up-core
	$(MAKE) cdc-up
	docker compose --profile cdc --profile streaming --profile dashboards up -d --build spark-cdc-stream grafana

dashboards-up: up-core
	docker compose --profile dashboards up -d --build --wait grafana

staging-init:
	docker compose run --rm staging-schema-init

clickhouse-users:
	docker compose up -d --wait clickhouse
	docker compose exec -T clickhouse bash /opt/clickhouse-setup/create_clickhouse_users.sh

warehouse-init:
	docker compose exec -T clickhouse sh -ec 'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --database "$$WAREHOUSE_DB_NAME" --multiquery' < db/warehouse_clickhouse/warehouse_ddl.sql

cdc-warehouse-init:
	docker compose exec -T clickhouse sh -ec 'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --database "$$WAREHOUSE_DB_NAME" --multiquery' < db/warehouse_clickhouse/cdc_warehouse_ddl.sql

warehouse-load: warehouse-init
	docker compose --profile batch run --rm spark-warehouse

status:
	docker compose ps

logs:
	docker compose logs -f --tail=100

test:
	PYTHONPATH=airflow/dags $(PYTHON) -m unittest discover -s tests/unit -v

down:
	docker compose down
