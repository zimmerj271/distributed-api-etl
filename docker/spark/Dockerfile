FROM apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu

USER root

# Install additional system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download Delta Lake and dependencies
ENV DELTA_VERSION=3.0.0
ENV SCALA_VERSION=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN curl -L -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -L -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Copy Spark configuration
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Install Python dependencies
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/spark/work

USER 185
