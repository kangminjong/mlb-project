MLB Data Pipeline (Airflow + Postgres + pybaseball)

pybaseball로 MLB 데이터를 수집해 JSON(raw)로 저장하고, Postgres에 upsert 적재하는 Airflow 파이프라인입니다.
운영 관점에서 아래 원칙을 우선으로 설계했습니다.

collect는 재시도 가능 (일시 장애/네트워크 오류 복구)

insert는 실패를 숨기지 않음 (retries=0, 파싱 오류는 Fail로 남김)

Discord 알림으로 실패 원인 추적(로그 링크 포함)

목적 / 설계 포인트

처음에는 statcast 단일 소스로 해결하려 했지만, 스키마/범위/사용성 측면에서 한계가 있었습니다.

batting/pitching stats, team_* 등 소스를 늘려서

수집/적재/분석 단계를 분리하기 쉬워졌고

이후 피처 엔지니어링/대시보드/모델링으로 확장하기 편한 형태가 됐습니다.

아키텍처

구성 요소

Airflow (CeleryExecutor)

Redis (Celery broker)

Postgres 2개

postgres: Airflow metadata DB

mlb_postgres: MLB 데이터 저장 DB

MLflow: docker-compose에 포함(옵션)

데이터 흐름

pybaseball → /opt/airflow/data/*.json (원자적 저장) → Postgres upsert

실행 방법
1) .env 준비

레포 루트에 .env 생성:

# Linux에서 권한 이슈 방지용(필요 시)
AIRFLOW_UID=

# MLB 데이터 DB
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# 내 PC에서 접근할 포트(호스트 포트)
DB_PORT=

# Discord Webhook (※ Airflow Variable로도 설정 필요)
DISCORD_WEBHOOK_URL=

# MLflow (옵션)
MLFLOW_TRACKING_URI=

DISCORD_WEBHOOK_URL은 환경변수만으로는 부족하고, Airflow UI의 Variables에도 동일하게 설정해야 합니다(아래 참고).

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

실패 시 Discord 알림에 포함되는 정보(예시)

dag_id / task_id / ds / run_id / try_number / log_url / error

2) Connection: my_db_connection

Airflow UI → Admin → Connections → +

Conn Id: my_db_connection

Conn Type: Postgres

Host: mlb_postgres ✅ (중요: localhost 아님)

Schema: ${POSTGRES_DB}

Login: ${POSTGRES_USER}

Password: ${POSTGRES_PASSWORD}

Port: 5432

컨테이너 내부에서 localhost는 “그 컨테이너 자기 자신”이라 DB 컨테이너로 연결되지 않습니다.
docker-compose 서비스명(mlb_postgres)로 접근해야 합니다.

DAG 설명
DAG	Schedule	목적	운영 원칙
my_collection_dag (Statcast)	@daily	일자별 statcast 수집 → 적재	collect는 재시도 / insert는 실패 숨기지 않음
my_stat_dag (Season Stats)	None (수동)	pitching/batting stats 수집 → 적재	postseason 데이터 없음은 정상 / JSON 파싱 오류는 Fail
운영 원칙(재시도 정책)
Collect(수집)

경기 없음/데이터 없음 → 정상 종료(return)

네트워크/일시 장애 등 → raise → Airflow retries로 복구

JSON 저장은 .tmp → os.replace로 원자적 저장 (깨진 JSON 생성 확률 감소)

Insert(적재)

insert task는 retries=0

FileNotFoundError → 데이터 없음으로 보고 Skip

JSONDecodeError → Fail(raise) + Discord 알림

깨진 JSON은 “데이터 없음”이 아니라 “파이프라인 오류”라서 숨기지 않음

Fail로 남겨야 log_url 기반으로 추적/재실행 가능

프로젝트 구조
dags/
  mlb_statcast_dag.py
  mlb_stat_dag.py
  common/
    config.py        # DEFAULT_ARGS, 시즌 기간 등
    callbacks.py     # Discord 알림
  src/
    collectors/      # pybaseball 수집 로직
    databases/       # Postgres 적재 로직(upsert, PK 누락 row skip)

docker-compose.yaml  # Airflow + Redis + Postgres + MLflow(옵션)
Dockerfile           # Airflow 이미지 커스텀(git 설치 포함)
requirements.txt     # 의존성
Backfill / 테스트 모드 메모

전 시즌 데이터를 처음 채우는 과정(backfill)은 실제 운영과 요구사항이 다릅니다.

초기 적재 단계에서는 “한 번에 채우는 작업”이 우선이라 테스트용 처리(기간 제한/일부 고정값 등)가 포함될 수 있습니다.

정규 시즌 운영 전환 시에는 아래를 정리할 예정입니다.

기간 제한 제거(또는 운영용 범위로 확장)

스케줄 기반 상시 실행으로 전환

테스트용 하드코딩 제거/단순화

포인트: “임시 처리”를 숨기지 않고, **왜 임시인지(초기 적재 1회성)**와 **언제 제거되는지(시즌 운영 전환 시점)**를 명시합니다.

트러블슈팅(자주 막히는 것들)
1) pybaseball 설치/동작이 불안정함

일부 환경에서 설치/동작이 불안정하여 GitHub 직접 설치 방식을 사용합니다.
그래서 Dockerfile에 git 설치가 포함되어 있어야 합니다.

2) Redis가 왜 필요함?

Executor가 CeleryExecutor라서 broker(redis)가 없으면 worker가 task를 받지 못합니다.
→ Redis는 필수 구성요소입니다.

3) DB Connection에서 localhost로 하면 연결 안 됨

컨테이너 내부에서 localhost는 “그 컨테이너 자기 자신”입니다.
→ mlb_postgres(서비스명)로 연결해야 정상입니다.

4) Postseason 파일이 없어서 적재가 스킵됨

시즌 초반엔 postseason 데이터가 비어 파일이 생성되지 않는 게 정상입니다.
그래서 insert에서 postseason 파일이 없으면 []로 처리하도록 구성합니다.

5) JSONDecodeError가 나는데 왜 Skip이 아니라 Fail임?

깨진 JSON은 “데이터 없음”이 아니라 “파이프라인 오류”라서 숨기면 누락이 발생합니다.
Fail로 남겨야 Discord 알림 + log_url로 추적/재실행이 가능합니다.

6) Permission denied (/opt/airflow/data, logs 등)

볼륨 마운트(./data, ./logs) 권한 문제일 수 있습니다.

(Linux) 필요 시 AIRFLOW_UID를 채우거나, 호스트 폴더 권한을 조정하세요.