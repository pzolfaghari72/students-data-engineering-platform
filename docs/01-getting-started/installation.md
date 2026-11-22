# Installation

Run from the repository root in a Linux shell. On WSL2 use the distribution that can run Docker Compose.

```bash
python3 --version
docker compose version
python3 -m venv .venv
make install
make init
```

`make install` targets the repository virtual environment when present. `make init` prepares the local project and creates `.env` from `.env.example` when needed. It fills generated service credentials and validates the local configuration. Enter the authorized source endpoint and reader credentials in the private `.env` file.

```bash
make compose-config
make test
```

The quiet Compose check avoids printing interpolated credentials. These commands do not start a pipeline. Use [Quick start](quick-start.md) to choose a service profile.

Image builds and JVM artifacts are separate from the host Python dependency installation. The Airflow image supplies Java 17 and Microsoft ODBC Driver 18. The Spark jobs resolve their named dependencies through `spark-submit`. Check build output for registry or Maven access problems before editing pinned versions. [Python and runtime dependencies](../09-development-and-testing/project-structure.md) · [Environment configuration](environment-configuration.md)
