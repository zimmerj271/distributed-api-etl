FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install additional Airflow providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-amazon==8.16.0

WORKDIR /opt/airflow
