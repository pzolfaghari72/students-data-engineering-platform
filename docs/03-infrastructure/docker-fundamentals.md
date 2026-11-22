# Docker and WSL2 fundamentals

An image packages a repeatable executable environment. A container is a running instance of an image, with its own processes, network and writable layer. A bind mount projects a repository path into a container. A named volume keeps state outside the disposable writable layer.

The project extends Airflow for Java and ODBC, Connect for its diagnostic JMX endpoint, Spark for Python clients, and Grafana for its pinned ClickHouse plugin. This makes the application and driver versions inspectable through the checked-in Dockerfiles. The host Python environment runs setup helpers and unit tests.

Docker uses a Linux kernel. On Windows, WSL2 provides a Linux environment in which to run Make and Compose. Keep the checkout on the Linux filesystem for bind-mount performance and file ownership. Confirm Docker integration for the distribution and run `id -u` to determine `AIRFLOW_UID` if a mounted log directory is inaccessible. For Spark, the mounted checkpoint root must permit the Spark user to create a lock and checkpoint. Fresh images prepare that directory, while an existing volume may need the upgrade procedure.

An image rebuild changes what a newly created container runs. An existing running container can still use the earlier image. Distinguish the image tag, container identity and application's runtime path when Java or Spark is missing. [Compose service design](docker-compose.md) · [Runtime troubleshooting](troubleshooting.md)
