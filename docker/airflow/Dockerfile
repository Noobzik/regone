FROM apache/airflow:2.3.2-python3.10
USER root
RUN apt-get update \
  && apt-get install -y libgdal-dev g++ --no-install-recommends \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
# RUN echo "airflow:airflow" | chpasswd && adduser airflow sudo
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
RUN pip install --no-cache-dir boto3 pandas pendulum xmlschema kafka-python partridge geopandas
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==3.0.0 apache-airflow-providers-amazon