from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

GOLD_RATE_URL = "https://metals-api.com/api/latest?access_key=ephizl03tdr5t4y4g1jnuez2y98lo1u8978pcogqvbfec16z12zf7paaewk2"

# path ที่จะใช้
gold_rate_output_path = "/home/airflow/gcs/data/gold_rate.csv"





def get_gold_rate(gold_rate_output_path):
    r = requests.get(GOLD_RATE_URL)
    result_gold_rate = r.json()
    df = pd.DataFrame(result_gold_rate)

    df = df.reset_index().rename(columns={"index": "G/S"})
    df['THBPrice'] = df['rates'] * 37.94

    df = df.drop("timestamp", axis=1)
    df = df.drop("success", axis=1)
    df = df.drop("unit", axis=1)
    
    df.to_csv(gold_rate_output_path, index=False)
    print(f"Output to {gold_rate_output_path}")





with DAG(
    "gold_final_dag",
    start_date=days_ago(1),
    schedule_interval= "@once",
    tags=["goldprice"]
) as dag:
  
    t1 = PythonOperator(
        task_id="gold_api",
        python_callable=get_gold_rate,
        op_kwargs={"gold_rate_output_path": gold_rate_output_path},
    )
    t2 = BashOperator(
        task_id="toBigQuery",
        bash_command="bq load \
            --source_format=CSV --autodetect \
            testKS.gold_data \
            gs://asia-east2-gold-price-73ce9869-bucket/data/gold_rate.csv"

    )

    t1 >> t2