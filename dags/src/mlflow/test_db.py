import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import traceback

# ==========================================
# 1. 환경 설정
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir))) 
env_path = os.path.join(project_root, '.env')

if os.path.exists(env_path):
    load_dotenv(env_path)

# URI 설정: WSL2에서 도커로 접근할 때는 localhost:5000이 기본입니다.
MLFLOW_URI = "http://127.0.0.1:5000"
MODEL_TYPE = "RF"
FEATURE_COLS = ['hits', 'doubles', 'triples', 'hr', 'rbi', 'bb', 'so', 'hbp', 'gdp']

# DB 설정
db_user = os.getenv("POSTGRES_USER", "minjong")
db_pw = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_URL = f"postgresql+psycopg2://{db_user}:{db_pw}@127.0.0.1:5432/mlb_stats"

def run_integrated_prediction():
    # MLflow 연결 강제 설정
    mlflow.set_tracking_uri(MLFLOW_URI)
    registered_name = f"MLB_OPS_{MODEL_TYPE}"
    engine = create_engine(DB_URL)
    client = MlflowClient()

    try:
        # --- [Step 1] 최신 모델 로드 ---
        print(f"🔎 [1/4] 모델 조회 중: {registered_name}...")
        
        # 최신 버전 검색 (Deprecated 함수 대신 search_model_versions 사용)
        filter_string = f"name='{registered_name}'"
        results = client.search_model_versions(filter_string)
        
        if not results:
            print(f"❌ 등록된 모델이 없습니다: {registered_name}")
            return
            
        # 가장 높은 버전 번호를 가진 모델 선택
        latest_info = max(results, key=lambda x: int(x.version))
        run_id = latest_info.run_id
        model_version = latest_info.version
        
        # 모델 URI 구성
        model_uri = f"models:/{registered_name}/{model_version}"
        print(f" -> ✅ 최신 버전 확인: v{model_version} (Run ID: {run_id})")
        
        # 모델 로드 (서버가 --serve-artifacts 상태면 원격으로 가져옴)
        model = mlflow.pyfunc.load_model(model_uri)
        print(f" -> ✅ 모델 로드 성공!")

        # --- [Step 2] 데이터 조회 ---
        print(f"📊 [2/4] 예측 대상 데이터(오타니 쇼헤이, 4월) 조회 중...")
        cols_str = ", ".join(FEATURE_COLS)
        query = text(f"""
            SELECT mlb_id, player_name, {cols_str} 
            FROM v_ml_ops_final_features 
            WHERE player_name = 'Shohei Ohtani' 
            AND EXTRACT(MONTH FROM game_date) = 4
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            print("⚠️ 예측할 데이터가 없습니다.")
            return

        # --- [Step 3] 예측 수행 ---
        print(f"🤖 [3/4] {MODEL_TYPE} 예측 모델 가동...")
        X_predict = df[FEATURE_COLS]
        daily_predictions = model.predict(X_predict)
        final_avg_ops = float(np.mean(daily_predictions).round(4))
        print(f" -> ✅ 예측 결과 (평균 OPS): {final_avg_ops}")

        # --- [Step 4] DB 저장 (Upsert) ---
        print(f"💾 [4/4] DB에 결과 저장 중...")
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        insert_data = {
            'player_id': int(df['mlb_id'].iloc[0]),
            'player_name': df['player_name'].iloc[0],
            'target_year': 2025,
            'predicted_ops': final_avg_ops,
            'model_name': registered_name,
            'run_id': run_id,
            'prediction_date': today_str
        }

        upsert_query = text("""
            INSERT INTO mlb_ops_predictions 
                (player_id, player_name, target_year, predicted_ops, model_name, mlflow_run_id, prediction_date)
            VALUES 
                (:player_id, :player_name, :target_year, :predicted_ops, :model_name, :run_id, :prediction_date)
            ON CONFLICT (player_id, target_year, model_name, prediction_date)
            DO UPDATE SET 
                predicted_ops = EXCLUDED.predicted_ops,
                mlflow_run_id = EXCLUDED.mlflow_run_id;
        """)

        with engine.begin() as conn:
            conn.execute(upsert_query, insert_data)

        print(f"\n🎉 모든 작업 완료! 예측값: {final_avg_ops}")

    except Exception as e:
        print(f"\n❌ 프로세스 오류 발생!")
        traceback.print_exc()

if __name__ == "__main__":
    run_integrated_prediction()