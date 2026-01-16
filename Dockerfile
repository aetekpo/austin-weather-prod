FROM quay.io/astronomer/ap-airflow:2.10.5-base
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dags /opt/airflow/dags

