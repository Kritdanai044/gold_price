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
# final_output_path = "asia-east2-gold-price-8f14e0b9-bucket/data/output.csv"





def get_gold_rate(gold_rate_output_path):
    r = requests.get(GOLD_RATE_URL)
    result_gold_rate = r.json()
    df = pd.DataFrame(result_gold_rate)

    # เปลี่ยนจาก index ที่เป็น date ให้เป็น column ชื่อ date แทน แล้วเซฟไฟล์ CSV
    df = df.reset_index().rename(columns={"index": "G/S"})
    df['THBPrice'] = df['rates'] * 37.94

    df = df.drop("timestamp", axis=1)
    df = df.drop("success", axis=1)
    df = df.drop("unit", axis=1)
    
    df.to_csv(gold_rate_output_path, index=False)
    print(f"Output to {gold_rate_output_path}")


# # def merge_data(transaction_path, conversion_rate_path, output_path):
#     # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
#     transaction = pd.read_csv(transaction_path)
#     conversion_rate = pd.read_csv(conversion_rate_path)

#     transaction['date'] = transaction['timestamp']
#     transaction['date'] = pd.to_datetime(transaction['date']).dt.date
#     conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

#     # merge 2 DataFrame
#     final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
#     # แปลงราคา โดยเอาเครื่องหมาย $ ออก และแปลงให้เป็น float
#     final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
#     final_df["Price"] = final_df["Price"].astype(float)

#     final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
#     final_df = final_df.drop(["date", "book_id"], axis=1)

#     # save ไฟล์ CSV
#     final_df.to_csv(output_path, index=False)
#     print(f"Output to {output_path}")
#     print("== End of Workshop 4 ʕ•́ᴥ•̀ʔっ♡ ==")


with DAG(
    "gold_final_dag",
    start_date=days_ago(1),
    schedule_interval= "@once",
    tags=["goldprice"]
) as dag:

    # TODO: สร้าง t1, t2, t3 ที่ใช้ PythonOperator 
    # และสร้าง task dependencies
    
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
            gs://asia-east2-gold-price-8f14e0b9-bucket/data/gold_rate.csv"

    )

    t1 >> t2