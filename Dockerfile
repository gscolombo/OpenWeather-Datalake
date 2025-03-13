FROM apache/airflow:2.10.5-python3.10

# Install dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt 

# Install Java (for Apache Spark)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless

# Load DAGs with source code
COPY --chown=airflow:root ./src/ /opt/airflow/dags

# Configure and initialize Apache Airflow
ENTRYPOINT  airflow db migrate && \
            airflow users create \
                --username admin \
                --password admin \
                --firstname Gabriel \
                --lastname Colombo \
                --role Admin \
                --email gscolombo404@gmail.com && \
            airflow scheduler & \
            airflow webserver

