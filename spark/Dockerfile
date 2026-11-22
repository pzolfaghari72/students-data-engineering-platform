FROM apache/spark:3.5.6

USER root
COPY requirements.txt /tmp/spark-requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/spark-requirements.txt \
    && install -d -o spark -g spark /opt/spark/checkpoints

USER spark
