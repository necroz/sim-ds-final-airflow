import datetime
import json
from pathlib import Path
import requests
from sqlalchemy import create_engine
import pendulum
import pandas as pd
import joblib

from airflow.sdk import dag, task

BASE_PATH = Path("/opt/airflow")  # Базовая директория проекта в Airflow
DATA_PATH = BASE_PATH / "data"    # Директория для данных (входных/выходных)
MODEL_PATH = BASE_PATH / "models" # Директория для моделей и конфигураций

@dag('sim-ds-basic-pipeline', schedule=datetime.timedelta(minutes=15), catchup=False,
    tags=["sim-ds-pipeline"],)
def data_pipeline():
    """
    Check for new data files
    preprocess it and make predictions
    """
    @task
    def check_for_db():
        #Check if db file exist and download it otherwise
        local_path = DATA_PATH / "delivery_final_homework.db"
        if not local_path.exists():
            url = "https://raw.githubusercontent.com/totiela/stepik_simulator_ds/main/lessons/final_project/delivery_final_homework.db"
            r = requests.get(url)
            r.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(r.content)
    @task
    def upload_data():
        #Get data from db
        local_path = DATA_PATH / "delivery_final_homework.db"
        engine = create_engine(f"sqlite:///{local_path}")
        df = pd.read_sql(
            """select *
            from orders as o
                    left join customers as c using (customer_id)
                    left join (select restaurant_id, restaurant_city as city, cuisine_type, is_fast_food, restaurant_rating, prep_time_avg from restaurants) as r using(restaurant_id) 
                    left join couriers as cr using(courier_id)
                    left join traffic as t using(city) 
                    left join weather as w using(city, date) where t.date = date(o.order_datetime);""",
            engine,
        )
        input_path = DATA_PATH / 'input'
        input_path.mkdir(exist_ok=True)
        df.to_csv(input_path / 'incoming.csv')
    
    @task
    def preprocess_data():
        #preprocess data and get necessary columns only
        df = pd.read_csv(DATA_PATH / 'input' / 'incoming.csv')
        
        drop_id_col = [
            "order_id",
            "customer_id",
            "courier_id",
            "restaurant_id",
        ]
        
        drop_unnecessary_dates_col = ["date", "start_date", "registration_date"]
        target = ["delivery_time_minutes"]
        columns = ['items_count', 'distance_km', 'is_express_delivery', 'is_fast_food', 'prep_time_avg', 'base_speed_kmh', 'traffic_level', 'precip_mm', 'city_num', 'vehicle_type_num']

        time_cols = ["order_datetime", "registration_date", "start_date", "date"] 
        for col in time_cols:
            df[col] = pd.to_datetime(df[col])
        df["precip_mm"] = df["precip_mm"].fillna(df["precip_mm"].median())
        df["wind_speed"] = df["wind_speed"].fillna(df["wind_speed"].median())
        df['traffic_level'] = df['traffic_level'].interpolate(method='linear')
        df["temperature"] = df["temperature"].interpolate(method="linear")
        cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
        for cat in cat_cols:
            df[cat] = df[cat].astype("category")
        bool_cols = ["is_express_delivery", "is_fast_food", "is_rush_hour"]
        for cat in bool_cols:
            df[cat] = df[cat].astype('bool')
            
        city_map = {"Moscow":0, "SPB":1, "Kazan":2}
        df['city_num'] = df['city'].map(city_map).astype('int64')
        
        vehicle_map = {"scooter":0, "bike":1, "car":2}
        df["vehicle_type_num"] = df["vehicle_type"].map(vehicle_map).astype("int64")
        
        df.drop(drop_id_col, axis=1, inplace=True)
        df.drop(drop_unnecessary_dates_col, axis=1, inplace=True)
        df.drop(cat_cols, axis=1, inplace=True)
        df.drop(target, axis=1, inplace=True)
        df  = df[columns]
        output_path = DATA_PATH / 'input'
        output_path.mkdir(exist_ok=True)
        df.to_csv(DATA_PATH / 'output' / 'processed.csv')
    
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["catboost==1.2.10", "joblib==1.5.3"], system_site_packages=False
    )
    def make_prediction():
        #load model, make prediction and store it to file
        import catboost
        import pandas as pd
        import joblib
        from pathlib import Path
        
        BASE_PATH = Path("/opt/airflow")  # Базовая директория проекта в Airflow
        DATA_PATH = BASE_PATH / "data"    # Директория для данных (входных/выходных)
        MODEL_PATH = BASE_PATH / "models" # Директория для моделей и конфигураций

        df = pd.read_csv(DATA_PATH / 'output' / 'processed.csv')
        model = joblib.load(MODEL_PATH / 'final_model.joblib')
        predictions = model.predict(df)
        pd.DataFrame(predictions).to_csv(DATA_PATH / 'output/prediction.csv')
    
    check_for_db() >> upload_data() >> preprocess_data() >> make_prediction()

dag = data_pipeline()