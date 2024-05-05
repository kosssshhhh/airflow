from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



from handsome.ops.products import FetchProductListFromCategoryOperator   


__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
__TAGS__ = ["handsome"]

with DAG(
    dag_id="handsome.items",
    start_date=pendulum.today("UTC"),
    schedule="30 19 * * *",
    default_args=__DEFAULT_ARGS__,
    catchup=False,
    tags=__TAGS__,
) as dag:
    start = EmptyOperator(task_id="start")
    
    """작업"""
    fetch_links = FetchProductListFromCategoryOperator(task_id='fetch_product_links')
    """작업"""
    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_links >> end
    