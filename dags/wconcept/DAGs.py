from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from wconcept.ops.load.product import (
    LoadWConceptProduct
)
# from wconcept.ops.load.review import (
#     LoadWConceptProductCategory
# )

from wconcept.ops.products import (
    FetchProductListFromCategoryOperator,
    FetchProductOperator
)

from wconcept.ops.reviews import (
    FetchReviewOperator
)

from wconcept.ops.images import (
    FetchImageOperator
)

__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
__TAGS__ = ["wconcept"]

with DAG(
    dag_id="wconcept.items",
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
    fetch_products_images = FetchImageOperator(task_id='fetch.products.images')
    """작업"""
    
    load_images = EmptyOperator(task_id="load.images")
    load_reviews = EmptyOperator(task_id="load.reviews")
    load_products = LoadWConceptProduct(task_id="load.products")
    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_products >> fetch_products_info >> load_products >> end
    start >> fetch_products >> fetch_products_images >> load_images >> end
    start >> fetch_products >> fetch_products_reviews >> load_reviews >> end
    
    
    