import json
from pathlib import Path
import joblib
import pendulum
from airflow.sdk import dag, task

BASE_PATH = Path("/opt/airflow")  # Базовая директория проекта в Airflow
DATA_PATH = BASE_PATH / "data"    # Директория для данных (входных/выходных)
MODEL_PATH = BASE_PATH / "models" # Директория для моделей и конфигураций

@dag()
def activate_model():
    model = joblib.load(MODEL_PATH / 'final_model.joblib')