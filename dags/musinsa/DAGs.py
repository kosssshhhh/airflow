from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
from datetime import datetime, timedelta

from musinsa.ops.load.product import (
    LoadMusinsaProduct,
)
from musinsa.ops.load.review import (
    LoadMusinsaReview
)
from musinsa.ops.products import (
    FetchProductListFromCategoryOperator,
    FetchProductOperator
)

from musinsa.ops.reviews import (
    FetchReviewOperator
)

from musinsa.ops.images import (
    FetchImageOperator
)

from musinsa.ops.transform_images import (
    TransformImageOperator
)

from musinsa.ops.load.image import (
    LoadMusinsaImage
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
    
    """"""
    fetch_products = FetchProductListFromCategoryOperator(task_id='fetch.products')
    fetch_products_info = FetchProductOperator(task_id='fetch.products.info')
    fetch_products_reviews = FetchReviewOperator(task_id='fetch.products.reviews')
    fetch_products_images = FetchImageOperator(task_id='fetch.products.images')
    transform_images = TransformImageOperator(
        task_id="transform.images",
        site_name="musinsa"
    )
    
    """작업"""
    

    load_images = LoadMusinsaImage(task_id="load.images")
    load_reviews = LoadMusinsaReview(task_id="load.reviews")
    load_products = LoadMusinsaProduct(task_id="load.products")

    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_products >> fetch_products_info >> load_products >> end
    start >> fetch_products >> fetch_products_images >> transform_images >> load_images >> end
    start >> fetch_products >> fetch_products_reviews >> load_reviews >> end
    
    
    