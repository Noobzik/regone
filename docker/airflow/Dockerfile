FROM apache/airflow:2.3.2-python3.10
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
RUN echo "airflow:airflow" | chpasswd && adduser airflow sudo
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3
RUN pip install boto3 pandas pendulum xmlschema kafka-python
