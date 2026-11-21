FROM apache/airflow:3.1.5

# Switch to root to install system-level dependencies.
USER root

# Install Microsoft ODBC Driver 18 for SQL Server.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgssapi-krb5-2 \
        openjdk-17-jre-headless \
        unixodbc \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Return to the non-root Airflow user before installing Python packages.
USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY --chown=airflow:root airflow/requirements.txt /tmp/airflow-requirements.txt
COPY --chown=airflow:root airflow/requirements-spark.txt /tmp/airflow-spark-requirements.txt

RUN pip install --no-cache-dir \
    --constraint "${HOME}/constraints.txt" \
    -r /tmp/airflow-requirements.txt

# Install Spark separately so Airflow constraints cannot upgrade PySpark or Py4J.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/airflow-spark-requirements.txt
