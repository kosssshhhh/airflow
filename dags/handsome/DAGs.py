from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



from handsome.ops.products import (
    FetchProductListFromCategoryOperator,
    FetchProductOperator, 
    
)

from handsome.ops.reviews import(
    FetchReviewOperator,
)


from handsome.ops.load.products import(
    LoadHandsomeProduct,
)

from handsome.ops.load.review import(
    LoadHandsomeReview
)
from handsome.ops.transform_images import(
    TransformImageOperator
)

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
    
    """"""
    
    fetch_products = FetchProductListFromCategoryOperator(task_id='fetch.products')
    fetch_products_info = FetchProductOperator(task_id='fetch.products.info')
    fetch_products_reviews = FetchReviewOperator(task_id='fetch.products.reviews')
    transform_images = TransformImageOperator(task_id="transform.images")
    """작업"""
    

    load_images = EmptyOperator(task_id="load.images")
    load_reviews = LoadHandsomeReview(task_id="load.reviews")
    load_products = LoadHandsomeProduct(task_id="load.products")

    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_products >> fetch_products_info >> load_products >> end # product info
    start >> fetch_products >> transform_images >> load_images >> end # product image
    start >> fetch_products >> fetch_products_reviews >> load_reviews >> end # product reviews
    
    
    