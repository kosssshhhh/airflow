from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import pendulum

from handsome.ops.styles import FetchStyleListFromCategoryOperator, FetchStyleOperator
from handsome.ops.reviews import FetchReviewOperator
from handsome.ops.load.styles import LoadHandsomeStyle
from handsome.ops.load.review import LoadHandsomeReview
from handsome.ops.load.image import LoadHandsomeImage
from handsome.ops.transform_images import TransformImageOperator
from handsome.ops.load.feature import LoadHandsomeFeature

from core.slack.slack_notification import SlackAlert

slack_api_token = Variable.get('slack_api_token')
alert = SlackAlert('#airflow-알리미', slack_api_token)

local_tz = pendulum.timezone("Asia/Seoul")

__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert.fail_msg,
    'on_success_callback': alert.success_msg
}
__TAGS__ = ["handsome"]

with DAG(
    dag_id="handsome.items",
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
    transform_images = TransformImageOperator(
        task_id="transform.images",
        site_name="handsome"
    )
    midterm = EmptyOperator(task_id="midterm")

    # load_features = LoadHandsomeFeature(task_id="load.features")
    load_features = EmptyOperator(task_id="load.features")
    
    load_images = LoadHandsomeImage(task_id="load.images")
    load_reviews = LoadHandsomeReview(task_id="load.reviews")
    load_styles = LoadHandsomeStyle(task_id="load.styles")

    
    end = EmptyOperator(task_id="end")
    
    load_tasks = [load_features, load_images, load_reviews]
    
    start >> fetch_styles >> fetch_styles_info >> load_styles

    fetch_styles >> fetch_styles_reviews
    fetch_styles >> transform_images

    fetch_styles_reviews >> midterm
    transform_images >> midterm
    load_styles >> midterm

    midterm >> load_tasks >> end

    