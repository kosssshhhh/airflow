# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# import pendulum
# from datetime import datetime, timedelta
# from airflow.models import Variable

# from musinsa_brand.ops.load.style import (
#     LoadMusinsaBrandStyle,
# )
# from musinsa_brand.ops.load.review import (
#     LoadMusinsaBrandReview
# )
# from musinsa_brand.ops.styles import (
#     FetchStyleListFromBrandOperator,
#     FetchStyleListAsyncFromBrandOperator,
#     FetchStyleBrandOperator,
#     FetchStyleAsyncBrandOperator
# )

# from musinsa_brand.ops.review_renewal import (
#     FetchReviewOperator
# )

# from musinsa_brand.ops.images import (
#     FetchImageOperator
# )

# from musinsa_brand.ops.transform_images import (
#     TransformImageOperator
# )

# from musinsa_brand.ops.load.image import (
#     LoadMusinsaBrandImage
# )

# # from musinsa_brand.ops.load.feature import (
# #     LoadMusinsaFeature
# # )


# from core.slack.slack_notification import SlackAlert

# slack_api_token = Variable.get('slack_api_token')
# alert = SlackAlert('#airflow-알리미', slack_api_token)

# local_tz = pendulum.timezone("Asia/Seoul")

# __DEFAULT_ARGS__ = {
#     'owner': '400CC',
#     'retries': 30,
#     'retry_delay': timedelta(minutes=5),
#     'on_failure_callback': alert.fail_msg,
#     'on_success_callback': alert.success_msg
# }
# __TAGS__ = ["musinsa_brand"]

# with DAG(
#     dag_id="musinsa.brand.items",
#     start_date= datetime(2024, 6, 18, tzinfo = local_tz),
#     schedule="5 9 * * *",
#     default_args=__DEFAULT_ARGS__,
#     catchup=False,
#     tags=__TAGS__,
# ) as dag:
#     start = EmptyOperator(task_id="start")
    
#     """"""
#     fetch_styles = FetchStyleListFromBrandOperator(task_id='fetch.styles')
#     fetch_styles_info = FetchStyleBrandOperator(task_id='fetch.styles.info')
#     fetch_styles_reviews = FetchReviewOperator(task_id='fetch.styles.reviews')
#     fetch_styles_images = FetchImageOperator(task_id='fetch.styles.images')
#     transform_images = TransformImageOperator(
#         task_id="transform.images", 
#         site_name="musinsa"
#     )
    
#     """작업"""
#     midterm = EmptyOperator(task_id="midterm")

#     # load_features = LoadMusinsaFeature(task_id="load.features")
#     load_features = EmptyOperator(task_id="load.features")
    
#     load_images = LoadMusinsaBrandImage(task_id="load.images")
#     load_reviews = LoadMusinsaBrandReview(task_id="load.reviews")
#     load_styles = LoadMusinsaBrandStyle(task_id="load.styles")
    
#     # load_images = EmptyOperator(task_id="load.images")
#     # load_reviews = EmptyOperator(task_id="load.reviews")
#     # load_styles = EmptyOperator(task_id="load.styles")

    
#     end = EmptyOperator(task_id="end")
    
#     load_tasks = [load_features, load_images, load_reviews]
    
#     start >> fetch_styles >> fetch_styles_info >> load_styles

#     fetch_styles >> fetch_styles_reviews
#     fetch_styles >> fetch_styles_images >> transform_images

#     fetch_styles_reviews >> midterm
#     transform_images >> midterm
#     load_styles >> midterm

#     midterm >> load_tasks >> end


# # with DAG(
# #     dag_id="musinsa.brand.items",
# #     start_date=datetime(2024, 6, 18, tzinfo=local_tz),
# #     schedule_interval="5 9 * * *",
# #     default_args=__DEFAULT_ARGS__,
# #     catchup=False,
# #     tags=__TAGS__,
# # ) as dag:
# #     start = EmptyOperator(task_id="start")

# #     fetch_styles = FetchStyleListAsyncFromBrandOperator(task_id='fetch.styles')
# #     fetch_styles_info = FetchStyleAsyncBrandOperator(task_id='fetch.styles.info')
# #     fetch_styles_reviews = FetchReviewOperator(task_id='fetch.styles.reviews')
# #     fetch_styles_images = FetchImageOperator(task_id='fetch.styles.images')
# #     transform_images = TransformImageOperator(
# #         task_id="transform.images", 
# #         site_name="musinsa"
# #     )

# #     load_images = LoadMusinsaBrandImage(task_id="load.images")
# #     load_reviews = LoadMusinsaBrandReview(task_id="load.reviews")
# #     load_styles = LoadMusinsaBrandStyle(task_id="load.styles")

# #     end = EmptyOperator(task_id="end")

# #     # Define task dependencies
# #     start >> fetch_styles >> fetch_styles_info >> load_styles
# #     fetch_styles >> fetch_styles_reviews
# #     fetch_styles >> fetch_styles_images >> transform_images
# #     transform_images >> load_images
# #     fetch_styles_reviews >> load_reviews

# #     load_styles >> end
# #     load_images >> end
# #     load_reviews >> end