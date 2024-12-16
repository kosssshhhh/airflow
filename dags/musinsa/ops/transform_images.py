import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from core.infra.cdn.image_uploader import ImageUploader

logger = logging.getLogger(__name__)

class TransformImageOperator(BaseOperator):
    @apply_defaults
    def __init__(self, site_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = "designovel"
        self.site_name = site_name

    def execute(self, context):
        task_instance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles.images", key="style_image_urls")
        
        uploader = ImageUploader(bucket_name=self.bucket_name, folder_name=self.site_name)
        image_url_list = uploader.upload_images(xcomData)
        
        context["task_instance"].xcom_push(key="cdn_image_urls", value=image_url_list)
