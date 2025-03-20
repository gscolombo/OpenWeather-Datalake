# Configuration of Airflow with Apache Spark
FROM apache/airflow:2.10.5-python3.10

# Install dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt 

# Install Java (for Apache Spark)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless

