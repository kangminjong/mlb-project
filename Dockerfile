# 1. 베이스 이미지: Airflow 최신 안정 버전 (Python 3.8 이상 권장)
FROM apache/airflow:2.9.3

# 2. 루트 권한으로 시스템 패키지 설치 (필요한 경우)
USER root

# git이나 vim 같은 기본 도구 설치 (디버깅용)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# [중요] 데이터 저장할 폴더 미리 만들고 권한 주기
# 이거 안 하면 "Permission Denied" 에러 나서 파일 저장 못 합니다!
RUN mkdir -p /opt/airflow/data \
    && chown -R airflow: /opt/airflow/data

# 3. 다시 airflow 유저로 전환 (필수)
USER airflow

# 4. 필요한 파이썬 라이브러리 설치
# requirements.txt 파일을 컨테이너 안으로 복사
COPY requirements.txt /requirements.txt

# pip 업그레이드 후 라이브러리 설치
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt