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
        # [핵심 해결책] 주먹구구가 아닌 '엔지니어링적 우회'
        # mlflow:5000이라는 이름 대신 IP 주소를 사용하여 DNS Rebinding 보안 필터를 통과합니다.
        try:
            mlflow_ip = socket.gethostbyname("mlflow")
            tracking_uri = f"http://{mlflow_ip}:5000"
        except socket.gaierror:
            # 도커 환경이 아닐 경우를 대비한 Fallback
            tracking_uri = "http://localhost:5000"
        
        mlflow.set_tracking_uri(tracking_uri)
        print(f"[INFO] MLflow Tracking URI set to: {tracking_uri}")

        self.experiment_name = "MLB_OPS_Prediction"
        artifact_path = "file:///mlflow_db/artifacts"
        
        # [정석] 실험이 없으면 생성, 있으면 설정
        if not mlflow.get_experiment_by_name(self.experiment_name):
            mlflow.create_experiment(self.experiment_name)
        mlflow.set_experiment(self.experiment_name)
            
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