FROM apache/airflow:2.11.0-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.11.0" -r /tmp/requirements.txt --constraint /home/airflow/constraints.txt && pip check

USER root
RUN mkdir -p /opt/project/work && chown -R airflow:root /opt/project && chmod -R g+rwX /opt/project
USER airflow

WORKDIR /opt/project
COPY --chown=airflow:root traffic.py ./
COPY --chown=airflow:root dags ./dags
COPY --chown=airflow:root sql ./sql

ENV PYTHONPATH=/opt/project
