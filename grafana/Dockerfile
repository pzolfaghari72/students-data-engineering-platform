ARG GRAFANA_VERSION=11.5.2
FROM grafana/grafana:${GRAFANA_VERSION}

USER root

ARG TARGETARCH
ARG CLICKHOUSE_PLUGIN_VERSION=4.12.0

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) plugin_arch=amd64; plugin_sha256=0ffa102944b60fde14bb32d9bf3da3df3b067158d84df7f20a127dd03fa9bc25 ;; \
      arm64) plugin_arch=arm64; plugin_sha256=aa2bb57869bb19c3aeb3046645debacb5fd925a7222d006dee50841ca6717abf ;; \
      arm) plugin_arch=arm; plugin_sha256=fa55e2a0b71cecb42708be8da928806a572075ecd9da8a3fe32c4683f2c7f6bf ;; \
      *) echo "Unsupported Grafana plugin architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    archive="grafana-clickhouse-datasource-${CLICKHOUSE_PLUGIN_VERSION}.linux_${plugin_arch}.zip"; \
    curl --fail --location --retry 3 \
      "https://github.com/grafana/clickhouse-datasource/releases/download/v${CLICKHOUSE_PLUGIN_VERSION}/${archive}" \
      --output "/tmp/${archive}"; \
    echo "${plugin_sha256}  /tmp/${archive}" | sha256sum -c -s; \
    mkdir -p /usr/share/grafana/plugins; \
    unzip -q "/tmp/${archive}" -d /usr/share/grafana/plugins; \
    chown grafana:root /usr/share/grafana/plugins; \
    chown -R grafana:root /usr/share/grafana/plugins/grafana-clickhouse-datasource; \
    rm "/tmp/${archive}"

USER grafana
