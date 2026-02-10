FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install Airflow providers and requests for REST API calls
RUN pip install --no-cache-dir \
    apache-airflow-providers-http==4.10.0 \
    apache-airflow-providers-amazon==8.16.0 \
    requests==2.31.0

WORKDIR /opt/airflow
