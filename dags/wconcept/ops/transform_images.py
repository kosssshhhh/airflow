import boto3
import logging
import io

from botocore.exceptions import ClientError
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.utils.context import Context

import urllib.request

logger = logging.getLogger(__name__)

class TransformImageOperator(BaseOperator):
        
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = S3Hook('aws.s3')  # connection ID 입력
        self.bucket_name = "designovellocal"
        self.s3_client = self.hook.get_conn()
        self.extra_args = {
            'ContentType': 'image/webp'
        }

    def execute(self, context):
        task_instance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products.images", key="product_imageURL")
        logger.info(f"xcomData : {xcomData}")
        
        image_url_list = self._load(xcomData)
        context["task_instance"].xcom_push(key="product_image_url", value=image_url_list)
    
    def _load(self, xcomData):
        url_dict = {}
        
        for key in xcomData:
            url_list = []
            file_name = "tmp.jpg"
            
            for i, url in enumerate(xcomData[key]):
                
                file_key = f"wconcept/{key}/{i}.webp"
                # Check if the file already exists in S3
                try:
                    self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
                    logger.info(f"File already exists: {file_key}. Skipping upload.")
                    url = f"https://{self.bucket_name}.s3.ap-northeast-2.amazonaws.com/{file_key}"
                    url_list.append(url)
                    continue
                except ClientError as e:
                    # The error signifies that the object does not exist
                    if e.response['Error']['Code'] == "404":
                        logger.info(f"Uploading file: {file_key}")
                        tmp = urllib.request.urlopen(url)
                        self.s3_client.upload_file(
                            File=io.BytesIO(tmp),
                            Bucket=self.bucket_name,
                            Key=file_key,
                            ExtraArgs=self.extra_args
                        )
                        url = f"https://{self.bucket_name}.s3.ap-northeast-2.amazonaws.com/{file_key}"
                        url_list.append(url)
                        
                
            url_dict[key] = url_list
            
        return url_dict