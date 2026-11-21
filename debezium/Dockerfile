FROM debezium/connect:2.7.3.Final

USER root
# Spark uses the exported snapshot-completion metric as a bootstrap gate.
ADD https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/kafka/libs/jmx_prometheus_javaagent.jar
COPY jmx_exporter.yml /opt/kafka/config/jmx_exporter.yml
