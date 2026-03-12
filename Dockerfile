FROM apache/airflow:2.8.1

# Chuyển sang quyền Root để cài đặt Java
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Trở lại user airflow mặc định và cài PySpark
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0