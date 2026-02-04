FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install additional system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download Delta Lake JARs
ENV DELTA_VERSION=3.0.0
ENV SCALA_VERSION=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -L -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER ${NB_UID}

# Install Python dependencies
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set environment variables for Spark
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar pyspark-shell"

WORKDIR /home/jovyan/work
