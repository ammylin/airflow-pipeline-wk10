FROM apache/airflow:3.1.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN mkdir -p /opt/airflow/data
COPY data/ /opt/airflow/data/

RUN chown -R airflow:root /opt/airflow/data
USER airflow