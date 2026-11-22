PYTHON ?= $(shell if test -x .venv/bin/python; then echo .venv/bin/python; else command -v python3; fi)
CDC_TABLES = $(shell PYTHONPATH=airflow/dags $(PYTHON) -B -c 'from students_staging.source_tables import SOURCE_TABLES; print(" ".join(table for table, _ in SOURCE_TABLES))')
export RUN_ID

define check-init
container=$$(docker compose $(1) ps --all --quiet $(2)) && \
test -n "$$container" && status=$$(docker wait "$$container") && \
{ test "$$status" = 0 || { printf '%s exited with status %s\n' '$(2)' "$$status" >&2; exit 1; }; }
endef

.PHONY: init install compose-config up up-core up-batch up-pipeline cdc-up streaming-up dashboards-up status logs test warehouse-load reconcile down

init:
	bash scripts/initialize_project.sh

install:
	$(PYTHON) -m pip install -r requirements.txt

compose-config:
	docker compose --profile cdc --profile streaming config --quiet

up: up-pipeline

up-core: init
	docker compose up -d staging-schema-init clickhouse-init
	@$(call check-init,,staging-schema-init)
	@$(call check-init,,clickhouse-init)

up-batch: init
	$(PYTHON) scripts/validate_environment.py --mode batch
	docker compose up -d --build --wait airflow-api-server airflow-scheduler airflow-dag-processor

up-pipeline: up-batch
	docker compose up -d --build --wait grafana

cdc-up: init
	$(PYTHON) scripts/validate_environment.py --mode cdc
	docker compose --profile cdc up -d --build --wait kafka connect
	docker compose --profile cdc exec -T kafka bash /opt/platform/prepare_cdc_topics.sh $(CDC_TABLES)
	docker compose --profile cdc up -d debezium-register
	@$(call check-init,--profile cdc,debezium-register)

streaming-up: cdc-up
	docker compose --profile cdc --profile streaming up -d --build --wait spark-cdc-stream grafana

dashboards-up: init
	docker compose up -d --build --wait grafana

warehouse-load:
	@case "$$RUN_ID" in ''|*[!0-9]*) printf 'Set RUN_ID to the successful staging run ID.\n' >&2; exit 1;; esac
	docker compose exec -T airflow-scheduler airflow dags trigger postgres_to_clickhouse_warehouse --conf "{\"source_run_id\": \"$$RUN_ID\"}"

reconcile:
	docker compose exec -T clickhouse sh -ec 'clickhouse-client --port "$$CLICKHOUSE_NATIVE_PORT" --user "$$GRAFANA_DB_USER" --password "$$GRAFANA_DB_PASSWORD" --database "$$WAREHOUSE_DB_NAME" --param_cdc_db="$$CDC_DB_NAME"' < db/warehouse_clickhouse/reconcile.sql

status:
	docker compose --profile cdc --profile streaming ps

logs:
	docker compose --profile cdc --profile streaming logs -f --tail=100

test:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=airflow/dags:spark $(PYTHON) -m unittest discover -s tests/unit -v

down:
	docker compose --profile cdc --profile streaming down
