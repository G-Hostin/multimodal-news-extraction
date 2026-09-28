# Image Airflow officielle + les dépendances de nos scripts qui n'y sont pas déjà
FROM apache/airflow:3.3.2

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt
