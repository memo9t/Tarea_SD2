FROM flink:1.17.1-scala_2.12

USER root

RUN apt-get update \
    && apt-get install -y python3 python3-pip python-is-python3 netcat-openbsd curl \
    && rm -rf /var/lib/apt/lists/*

COPY flink_job/requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

RUN wget -qO /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

COPY core_lib /opt/flink/core_lib
COPY flink_job/stream_quality_job.py /opt/flink/usrlib/stream_quality_job.py

ENV PYTHONPATH=/opt/flink/usrlib:/opt/flink:/opt/flink/core_lib

USER flink

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["jobmanager"]
