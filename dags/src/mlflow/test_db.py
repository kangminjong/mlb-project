import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import sys
from datetime import datetime
# ==========================================
# 1. 환경 설정 및 변수 로드
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir))) 
env_path = os.path.join(project_root, '.env')

if os.path.exists(env_path):
    load_dotenv(env_path)

# MLflow 설정
MLFLOW_URI = "http://localhost:8081" 
MODEL_TYPE = "XGB"
VERSION = "latest"
FEATURE_COLS = ['hits', 'doubles', 'triples', 'hr', 'rbi', 'bb', 'so', 'hbp', 'gdp']

# DB 설정
id = os.getenv("POSTGRES_USER", "airflow")
pw = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_URL = f"postgresql+psycopg2://{id}:{pw}@localhost:5432/mlb_stats"

# ==========================================
# 2. 헬퍼 함수: 물리적 경로 탐색 (이건 유지해야 파일 찾습니다)
# ==========================================
def search_folder_recursive(root_path, target_names):
    print(f"🔎 [Search] 탐색 시작... (대상: {target_names})")
    found_paths = []
    for root, dirs, files in os.walk(root_path):
        for dirname in dirs:
            if dirname in target_names:
                full_path = os.path.join(root, dirname)
                found_paths.append(full_path)
    
    for p in found_paths:
        if "MLmodel" in os.listdir(p):
            return p
        for r, d, f in os.walk(p):
            if "MLmodel" in f:
                return r 
    return None

# ==========================================
# 3. 실행 함수
# ==========================================
def run_integrated_prediction():
    mlflow.set_tracking_uri(MLFLOW_URI)
    registered_name = f"MLB_OPS_{MODEL_TYPE}"
    engine = create_engine(DB_URL)

    try:
        # [Step 1] 모델 경로 찾기
        print(f"--- [1/4] 모델 경로 찾는 중 ---")
        client = MlflowClient()
        try:
            model_infos = client.get_latest_versions(registered_name, stages=["None", "Staging", "Production"])
            latest_model = sorted(model_infos, key=lambda x: int(x.version))[-1]
        except:
            print(f"❌ 모델을 찾을 수 없습니다.")
            return

        run_id = latest_model.run_id
        source = latest_model.source
        search_targets = [run_id]
        if source and "models:/" in source:
            search_targets.append(source.split("/")[-1])
        
        local_root = os.path.join(project_root, "mlflow_data")
        local_model_path = search_folder_recursive(local_root, search_targets)

        if not local_model_path:
            print("❌ 모델 폴더 탐색 실패")
            return
        
        print(f" -> ✅ 경로 확보: {local_model_path}")
        model = mlflow.pyfunc.load_model(local_model_path)

        # [Step 2] 데이터 조회
        print(f"--- [2/4] 데이터 조회 중 ---")
        cols_str = ", ".join(FEATURE_COLS)
        query = f"""
            SELECT mlb_id, player_name, {cols_str} 
            FROM v_ml_ops_final_features 
            WHERE player_name = 'Shohei Ohtani' 
            AND EXTRACT(MONTH FROM game_date) = 4
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        if df.empty:
            print("⚠️ 데이터 없음")
            return

        # [Step 3] 예측
        print(f"--- [3/4] 예측 수행 중 ---")
        X_predict = df[FEATURE_COLS]
        daily_predictions = model.predict(X_predict)
        final_avg_ops = float(np.mean(daily_predictions).round(4))

        # [Step 4] DB 적재 (일반 INSERT로 수정됨)
        print(f"--- [4/4] 결과 저장 중 (Simple INSERT) ---")
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

        # [수정됨] ON CONFLICT 구문 제거 -> 무조건 INSERT
        insert_query = text("""
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
            conn.execute(insert_query, insert_data)

        print(f"\n🎉 저장 완료! [{registered_name}] 예측값: {final_avg_ops}")

    except Exception as e:
        print(f"\n❌ 에러 발생: {e}")

if __name__ == "__main__":
    run_integrated_prediction()