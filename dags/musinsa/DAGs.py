from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum

# from musinsa.ops.reviews import FetchReviewOperator
from musinsa.ops.load.style import LoadMusinsaStyle
from musinsa.ops.load.review import LoadMusinsaReview
from musinsa.ops.styles import FetchStyleListFromCategoryOperator, FetchStyleOperator
from musinsa.ops.review_renewal import FetchReviewOperator
from musinsa.ops.images import FetchImageOperator
from musinsa.ops.transform_images import TransformImageOperator
from musinsa.ops.load.image import LoadMusinsaImage
from musinsa.ops.load.feature import LoadMusinsaFeature

from core.slack.slack_notification import SlackAlert

slack_api_token = Variable.get('slack_api_token')
alert = SlackAlert('#airflow-알리미', slack_api_token)

local_tz = pendulum.timezone("Asia/Seoul")

__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 40,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert.fail_msg,
    'on_success_callback': alert.success_msg
}
__TAGS__ = ["musinsa"]

with DAG(
    dag_id="musinsa.items",
    start_date= datetime(2024, 6, 18, tzinfo = local_tz),
    schedule="5 9 * * *",
    default_args=__DEFAULT_ARGS__,
    catchup=False,
    tags=__TAGS__,
) as dag:
    start = EmptyOperator(task_id="start")
    
    fetch_styles = FetchStyleListFromCategoryOperator(task_id='fetch.styles')
    fetch_styles_info = FetchStyleOperator(task_id='fetch.styles.info')
    fetch_styles_reviews = FetchReviewOperator(task_id='fetch.styles.reviews')
    fetch_styles_images = FetchImageOperator(task_id='fetch.styles.images')
    transform_images = TransformImageOperator(
        task_id="transform.images", 
        site_name="musinsa"
    )
    
    midterm = EmptyOperator(task_id="midterm")

    # load_features = LoadMusinsaFeature(task_id="load.features")
    load_features = EmptyOperator(task_id="load.features")
    load_images = LoadMusinsaImage(task_id="load.images")
    load_reviews = LoadMusinsaReview(task_id="load.reviews")
    load_styles = LoadMusinsaStyle(task_id="load.styles")

    
    end = EmptyOperator(task_id="end")
    
    load_tasks = [load_features, load_images, load_reviews]
    
    start >> fetch_styles >> fetch_styles_info >> load_styles

    fetch_styles >> fetch_styles_reviews
    fetch_styles >> fetch_styles_images >> transform_images

    fetch_styles_reviews >> midterm
    transform_images >> midterm
    load_styles >> midterm

    midterm >> load_tasks >> end