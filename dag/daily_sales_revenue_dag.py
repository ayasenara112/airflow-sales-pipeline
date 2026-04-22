from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import matplotlib.pyplot as plt
import pandas as pd
import os

# PostgreSQL connection ID configured in Airflow
PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# output folder
OUTPUT_DIR = "/tmp/daily_sales_pipeline"
os.makedirs(OUTPUT_DIR, exist_ok=True)

RAW_CSV = os.path.join(OUTPUT_DIR, "raw_sales.csv")
DAILY_CSV = os.path.join(OUTPUT_DIR, "daily_revenue.csv")
PLOT_FILE = os.path.join(OUTPUT_DIR, "daily_revenue_plot.png")

EXTRACT_SQL = """
    SELECT
        o.OrderDate::date AS order_day,
        od.Quantity AS quantity,
        p.Price AS unit_price
    FROM orders o
    JOIN order_details od
        ON o.OrderID = od.OrderID
    JOIN products p
        ON od.ProductID = p.ProductID
    WHERE o.OrderDate IS NOT NULL
"""

def extract_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    df = hook.get_pandas_df(EXTRACT_SQL)
    df.to_csv(RAW_CSV, index=False)

def get_total_revenue():
    df = pd.read_csv(RAW_CSV)

    df["revenue"] = df["quantity"] * df["unit_price"]

    revenue_df = (
        df.groupby("order_day", as_index=False)["revenue"]
        .sum()
        .sort_values("order_day")
        .rename(columns={"revenue": "daily_revenue"})
    )

    revenue_df.to_csv(DAILY_CSV, index=False)

def plot_daily_revenue():
    revenue_df = pd.read_csv(DAILY_CSV)
    revenue_df["order_day"] = pd.to_datetime(revenue_df["order_day"])

    plt.figure(figsize=(10, 5))
    plt.plot(revenue_df["order_day"], revenue_df["daily_revenue"], marker="o")
    plt.title("Daily Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(PLOT_FILE)
    plt.close()

with DAG(
    dag_id="daily_sales_revenue_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    description="Daily sales revenue pipeline from PostgreSQL using Airflow",
) as dag:

    t1 = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_data
    )

    t2 = PythonOperator(
        task_id="calculate_daily_revenue",
        python_callable=get_total_revenue
    )

    t3 = PythonOperator(
        task_id="plot_daily_revenue",
        python_callable=plot_daily_revenue
    )

    t1 >> t2 >> t3
