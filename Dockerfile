FROM apache/airflow:2.9.3

USER root

# build 시 git 설치가 필요한 경우(예: git+https 로 패키지 설치)
RUN apt-get update \
  && apt-get install -y --no-install-recommends git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Airflow 컨테이너 내부 데이터 저장 경로 + 권한
RUN mkdir -p /opt/airflow/data \
  && chown -R airflow:root /opt/airflow/data \
  && chmod -R 775 /opt/airflow/data

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt