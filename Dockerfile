FROM apache/airflow:latest
RUN pip install pandas
RUN pip install 'apache-airflow-providers-sendgrid'