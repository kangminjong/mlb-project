# MLB Data Pipeline (Airflow + Postgres + pybaseball)

pybaseball로 MLB 데이터를 수집하고(JSON raw 저장), Postgres에 적재하는 Airflow 파이프라인입니다.  
운영 관점에서 **재실행(collect 재시도)** / **누락 숨기지 않기(insert fail)** / **Discord 알림(로그 링크 포함)**을 핵심으로 정리했습니다.

---

## 목적 / 설계 포인트

- 처음에는 `statcast` 하나로 모든 걸 해결하려다(스키마/범위/사용성 측면에서) 한계가 있었고,
- `batting/pitching stats`, `team_*` 등으로 데이터 소스를 늘리니
  - 적재/분석 작업을 분리하기 쉬워졌고
  - 이후 다른 작업(피처 엔지니어링/대시보드/모델링)에 더 편했습니다.

---

## 아키텍처

- Airflow (CeleryExecutor)
- Redis (Celery broker)
- Postgres 2개
  - `postgres`: Airflow metadata DB
  - `mlb_postgres`: MLB 데이터 저장 DB
- MLflow: compose에 포함(옵션)

데이터 흐름
- `pybaseball → /opt/airflow/data/*.json(원자적 저장) → Postgres upsert`

---

## 실행 방법

### 1) `.env` 준비
레포 루트에 `.env` 생성:

```env
AIRFLOW_UID=

# MLB 데이터 DB
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# 내 PC에서 접근할 포트
DB_PORT=

DISCORD_WEBHOOK_URL=

MLFLOW_TRACKING_URI=

DISCORD_WEBHOOK_URL은 Airflow Variable로도 설정 필요(아래 참고)

2) 실행
docker compose up -d --build
3) 접속

Airflow: http://localhost:8080

기본 계정: admin / admin

MLflow(옵션): http://localhost:5000

Airflow 필수 설정
1) Variable: DISCORD_WEBHOOK_URL

Airflow UI → Admin → Variables

Key: DISCORD_WEBHOOK_URL

Value: Discord Webhook URL

실패 시 Discord 알림에 아래 정보가 포함됩니다:
dag_id / task_id / ds / run_id / try_number / log_url / error

2) Connection: my_db_connection

Airflow UI → Admin → Connections → +

Conn Id: my_db_connection

Conn Type: Postgres

Host: mlb_postgres ← 중요: localhost 아님

Schema: ${POSTGRES_DB}

Login: ${POSTGRES_USER}

Password: ${POSTGRES_PASSWORD}

Port: 5432

DAG 설명
1) my_collection_dag (Statcast, 일 단위)

schedule: @daily

핵심: 일자별 statcast 수집 → 적재

운영: collect는 재시도(retries) 가능 / insert는 retries=0

2) my_stat_dag (Season Stats, 수동)

schedule: None (수동 실행)

pitching/batting stats(Regular/Post) 수집 → 적재

Postseason 파일이 없으면 빈 리스트 처리(정상)

JSON 파싱 깨짐은 Fail로 남김(숨기지 않음)

재실행(재시도) 원칙
Collect(수집)

경기 없음/데이터 없음 → 정상 종료(return)

그 외 오류(네트워크/일시 장애 등) → raise → Airflow retries로 복구

JSON 저장은 .tmp → os.replace로 원자적 저장(깨진 JSON 생성 확률 감소)

Insert(적재)

insert task는 retries=0

FileNotFoundError는 데이터 없음으로 보고 스킵

JSONDecodeError는 Fail(raise) + Discord 알림
→ 누락을 숨기지 않고 원인 추적 가능(Discord log_url)

프로젝트 구조(요약)

dags/

mlb_statcast_dag.py

mlb_stat_dag.py

common/ : config, callbacks(Discord 알림)

src/collectors/ : pybaseball 수집 로직

src/databases/ : Postgres 적재 로직(upsert, PK 누락 row skip)

docker-compose.yaml : Airflow + Redis + Postgres + MLflow

Dockerfile : Airflow 이미지 커스텀(git 설치 포함)

requirements.txt : 의존성(※ pybaseball을 GitHub로 설치하기 위해 git 필요)

트러블슈팅(자주 막히는 것들)
1) pybaseball 설치가 안 됨

일부 환경에서 pybaseball이 정상 설치/동작이 불안정해서 GitHub 직접 설치 방식 사용

그래서 Dockerfile에 git 설치가 포함되어 있어야 함

2) Redis가 왜 필요함?

executor가 CeleryExecutor라서 broker(redis)가 없으면 worker가 task를 못 받음
→ Redis는 필수 구성요소

3) DB Connection에서 localhost로 하면 연결 안 됨

컨테이너 내부에서 localhost는 “그 컨테이너 자기 자신”

mlb_postgres(서비스명) 로 연결해야 정상

4) Postseason 파일이 없어서 적재가 스킵됨

시즌 초반엔 postseason 데이터가 비어 파일이 생성되지 않는 게 정상

그래서 insert에서 postseason 파일은 없으면 []로 처리하도록 구성

5) JSONDecodeError가 나는데 왜 스킵이 아니라 Fail임?

깨진 JSON은 “데이터 없음”이 아니라 “파이프라인 오류”라서 숨기면 누락이 발생

Fail로 남겨야 Discord 알림 + log_url로 추적/재실행이 가능

6) Permission denied (/opt/airflow/data, logs 등)

볼륨 마운트(./data, ./logs) 권한 문제일 수 있음

(Linux) 필요 시 AIRFLOW_UID를 채우거나, 호스트 폴더 권한을 조정