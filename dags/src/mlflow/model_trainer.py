import mlflow
import pandas as pd
import numpy as np
import socket
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score


class Modeltrainer:
    def __init__(self):
        # [핵심 수정] 복잡한 IP 조회 로직 제거 -> 환경변수 기반의 정석 설정
        # 1. Docker Compose에서 설정한 환경변수(MLFLOW_TRACKING_URI)를 우선 사용
        # 2. 없으면 로컬 개발환경으로 간주하고 localhost 사용
        self.tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        
        mlflow.set_tracking_uri(self.tracking_uri)
        print(f"[INFO] MLflow Tracking URI set to: {self.tracking_uri}")

        self.experiment_name = "MLB_OPS_Prediction"
        
        # [안정성 강화] 실험 생성 및 설정 로직
        try:
            # 실험이 이미 있는지 확인하고, 없으면 생성합니다.
            if not mlflow.get_experiment_by_name(self.experiment_name):
                print(f"[INFO] Creating new experiment: {self.experiment_name}")
                mlflow.create_experiment(self.experiment_name)
            
            # 해당 실험을 활성화합니다.
            mlflow.set_experiment(self.experiment_name)
            print(f"[INFO] Active experiment set to: {self.experiment_name}")
            
        except Exception as e:
            # MLflow 서버가 아직 준비되지 않았을 때 DAG 전체가 죽는 것을 방지
            print(f"[WARN] Failed to connect to MLflow or set experiment. Error: {e}")
            print(f"[WARN] Training will continue, but logging might fail.")

        # 모델 학습에 사용할 Feature 정의
        self.feature_cols = ['hits', 'doubles', 'triples', 'hr', 'rbi', 'bb', 'so', 'hbp', 'gdp']
        self.target_col = 'target_ops'
    
    def prepare_features(self, pg_hook):
        # [수정] SQL 구문 가독성 및 정합성 확보
        cols_str = ", ".join(self.feature_cols)
        query = f"SELECT {cols_str}, {self.target_col} FROM v_ml_ops_final_features"
        
        df = pg_hook.get_pandas_df(query)
        print(f"[INFO] Data loaded: {len(df)} rows")

        X = df[self.feature_cols]
        y = df[self.target_col]

        return train_test_split(X, y, test_size=0.2, random_state=42)

    def train_and_log(self, X_train, X_test, y_train, y_test, model_type="RF"):
        today = datetime.today().strftime('%Y-%m-%d %H:%M:%S') 
        registered_name = f"MLB_OPS_{model_type}"

        # MLflow Run 시작
        with mlflow.start_run(run_name=f"{model_type}_{today}"):
            if model_type == "RF":
                model = RandomForestRegressor(n_estimators=100, random_state=42)
                artifact_path = "rf_model"
                mlflow.log_param("n_estimators", 100)
                mlflow.log_param("model_class", "RandomForest")
                log_func = mlflow.sklearn.log_model
            else:
                model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
                artifact_path = "xgb_model"
                mlflow.log_param("learning_rate", 0.1)
                mlflow.log_param("n_estimators", 100)
                mlflow.log_param("model_class", "XGBoost")
                log_func = mlflow.xgboost.log_model
            
            # 모델 학습
            model.fit(X_train, y_train)
            predictions = model.predict(X_test)

            # 평가지표 계산
            rmse = np.sqrt(mean_squared_error(y_test, predictions))
            r2 = r2_score(y_test, predictions)

            # MLflow 로깅
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("r2_score", r2)
            
            # 모델 저장 및 레지스트리 등록
            log_func(
                model, 
                artifact_path=artifact_path, 
                registered_model_name=registered_name
            )
            print(f"[{model_type}] MLflow 로깅 및 모델 등록 완료 (RMSE: {rmse:.4f})")

    def predict_and_insert(self, model_type, version, pg_hook):
        registered_name = f"MLB_OPS_{model_type}"
        model_uri = f"models:/{registered_name}/{version}"
        
        # [주의] pyfunc로 로드하여 범용적인 predict 인터페이스 확보
        model = mlflow.pyfunc.load_model(model_uri)

        # 예측용 데이터 쿼리 (도메인 지식을 반영한 특정 시점/선수 필터링)
        cols_str = ", ".join(self.feature_cols)
        query = f"""
            SELECT mlb_id, player_name, {cols_str} 
            FROM v_ml_ops_final_features 
            WHERE player_name = 'Shohei Ohtani' 
            AND EXTRACT(MONTH FROM game_date) = 4
        """
        df = pg_hook.get_pandas_df(query)

        if df.empty:
            print("[WARN] No data found for prediction.")
            return None

        # 예측 수행
        X_predict = df[self.feature_cols]
        daily_predictions = model.predict(X_predict)
        final_avg_ops = float(np.mean(daily_predictions).round(4))

        # 결과 저장용 DataFrame 생성 (DB 스키마와 매칭)
        result_df = pd.DataFrame([{
            'player_id': int(df['mlb_id'].iloc[0]),
            'player_name': df['player_name'].iloc[0],
            'target_year': 2025,
            'predicted_ops': final_avg_ops,
            'model_name': registered_name,
            'mlflow_run_id': str(model.metadata.run_id)
        }])

        # PostgreSQL 데이터 삽입
        pg_hook.insert_rows(
            table='mlb_ops_predictions',
            rows=result_df.values.tolist(),
            target_fields=['player_id', 'player_name', 'target_year', 'predicted_ops', 'model_name', 'mlflow_run_id']
        )
        print(f"[{registered_name}] DB 적재 완료: {final_avg_ops}")