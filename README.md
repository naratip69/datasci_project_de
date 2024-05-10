To start

Clone this repo

Insert your scopus api key in .env

If you os is not linux set AIRFLOW_UID=50000
If you use linux run command: echo -e $(id -u) -> set this number to AIRFLOW_UID

Run this command: sudo chmod -R 777 dags/
Then
Run this command: sudo chmod -R 777 logs/
Then
Run this command: docker build . --tag extending_airflow:latest
Then
Run this command: docker compose up airflow-init
Then
Run this command: docker compose up -d

To open airflow using
localhost:8080

username: airflow
password: airflow

