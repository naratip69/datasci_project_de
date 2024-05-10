FROM apache/airflow:2.9.0
COPY requirements.txt  /
COPY .env /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt


#docker build . --tag extending_airflow:latest