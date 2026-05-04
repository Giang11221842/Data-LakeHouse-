FROM apache/airflow:2.7.1-python3.11

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Pre-download Spark JARs so workers never hit Maven Central at runtime.
# Placing them under /opt/spark/jars/ which is referenced by spark.jars in the ETL code.
RUN mkdir -p /opt/spark/jars \
    && curl -fSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fSL -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.76.3.jar \
        https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.76.3/nessie-spark-extensions-3.5_2.12-0.76.3.jar \
    && chmod -R 644 /opt/spark/jars/*.jar

# Download static reference data used by gold_transform (taxi zone lookup).
# Stored outside the /opt/airflow/data volume mount so it is not overwritten at runtime.
RUN mkdir -p /opt/airflow/resources \
    && curl -fSL -o /opt/airflow/resources/taxi_zone_lookup.csv \
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv" \
    || curl -fSL -o /opt/airflow/resources/taxi_zone_lookup.csv \
        "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

USER airflow

RUN pip install -r /opt/airflow/requirements.txt
