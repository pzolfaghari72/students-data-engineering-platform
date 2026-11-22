"""Exercise the Spark warehouse publisher against disposable fixture databases."""

import argparse
import logging
import os

import clickhouse_connect
from pyspark.sql import SparkSession

from load_warehouse import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--empty-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not os.environ["WAREHOUSE_DB_NAME"].startswith("students_fixture_"):
        raise RuntimeError("Use a dedicated scratch ClickHouse database.")
    if "/students_fixture_" not in os.environ["STAGING_JDBC_URL"]:
        raise RuntimeError("Use a dedicated scratch PostgreSQL database.")
    spark = (SparkSession.builder.appName("Students batch publication fixture")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        with clickhouse_connect.get_client(
            host=os.environ["WAREHOUSE_HOST"], port=int(os.environ["WAREHOUSE_HTTP_PORT"]),
            database=os.environ["WAREHOUSE_DB_NAME"], username=os.environ["WAREHOUSE_DB_USER"],
            password=os.environ["WAREHOUSE_DB_PASSWORD"],
        ) as client:
            assert client.query("SELECT count() FROM warehouse_model_loads").first_row[0] == 0
            if not args.empty_only:
                run(spark, client, 1)
                assert tuple(client.query(
                    "SELECT source_run_id, fact_application_rows, fact_course_offering_rows, "
                    "fact_enrollment_rows FROM batch_publication_status"
                ).first_row) == (1, 1, 1, 1)
                assert client.query("SELECT sum(capacity) FROM fact_course_offering").first_row[0] == 12
                run(spark, client, 1)
                assert client.query("SELECT count() FROM warehouse_model_loads").first_row[0] == 1
            run(spark, client, 2)
            assert tuple(client.query(
                "SELECT source_run_id, fact_application_rows, fact_course_offering_rows, "
                "fact_enrollment_rows FROM batch_publication_status"
            ).first_row) == (2, 0, 0, 0)
            suffix = "empty publication" if args.empty_only else "pinned source run, retry, and empty publication"
            print(f"Spark batch fixture passed: {suffix}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
