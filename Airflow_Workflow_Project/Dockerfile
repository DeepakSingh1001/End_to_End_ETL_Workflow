FROM apache/airflow:3.1.5

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

USER airflow

# Install PySpark (includes spark-submit)
RUN pip install pyspark==3.5.1
