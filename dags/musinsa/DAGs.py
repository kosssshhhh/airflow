from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from musinsa.ops.products import (
    FetchProductLinksOperator,
    FetchProductDetailsOperator,
)

from musinsa.ops.reviews import (
    printOperator,    
)
__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
__TAGS__ = ["musinsa"]

with DAG(
    dag_id="musinsa.items",
    start_date=pendulum.today("UTC"),
    schedule="30 19 * * *",
    default_args=__DEFAULT_ARGS__,
    catchup=False,
    tags=__TAGS__,
) as dag:
    start = EmptyOperator(task_id="start")
    
    fetch_links = FetchProductLinksOperator(task_id='fetch_product_links', category_nums=['001006', '001004'])
    fetch_details = FetchProductDetailsOperator(task_id='fetch_product_details', product_urls="{{ ti.xcom_pull(task_ids='fetch_product_links', key='product_links') }}")
    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_links >> fetch_details >> end
    