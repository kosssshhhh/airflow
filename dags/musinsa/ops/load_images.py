import boto3
import logging
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.utils.context import Context

import urllib.request

logger = logging.getLogger(__name__)

class ImageUploadOperator(BaseOperator):
        
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
        xcomData = task_instance.xcom_pull(task_ids="fetch.products.images", key="product_image")
        logger.info(f"xcomData : {xcomData}")
        
        image_url_list = self._load(xcomData)
        context["task_instance"].xcom_push(key="product_image_url", value=image_url_list)
    
    def _load(self, xcomData):
        url_dict = {}
        
        for key in xcomData:
            
            url_list = []
            
            for i, url in enumerate(xcomData[key]):
                file_name = "tmp.jpg"
                file_key = f"musinsa/{key}/{i}.webp"
                urllib.request.urlretrieve(url, file_name)

                self.s3_client.upload_file(
                    Filename=file_name,
                    Bucket=self.bucket_name,
                    Key=file_key,
                    ExtraArgs=self.extra_args
                )
                url = f"https://{self.bucket_name}.s3.ap-northeast-2.amazonaws.com/{file_key}"
                url_list.append(url)
                
                os.remove(file_name)
                
            url_dict[key] = url_list
            
        return url_dict
                
        

# import boto3
# import logging
# import os

# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
# from airflow.utils.context import Context

# import urllib.request

# logger = logging.getLogger(__name__)


# class ImageUploadOperator(BaseOperator):
   
#     @apply_defaults
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.hook = S3Hook('aws.s3')  # connection ID 입력
#         self.bucket_name = "designovellocal"
    
#     def execute(self, context: Context):
#         task_instance = context["task_instance"]
#         xcomData = task_instance.xcom_pull(task_ids="fetch.products.images", key="product_image")
#         logger.info(f"xcomData : {xcomData}")
        
#         self._load(xcomData)
    
#     def _load(self, xcomData):
#         for key in xcomData:
#             for i, url in enumerate(xcomData[key]):
#                 file_name = "tmp.webp"
#                 file_key = f"musinsa/{key}/{i}.webp"
#                 file_path = "dags/musinsa/files"
#                 urllib.request.urlretrieve(url, file_name)
#                 self.hook.load_file(
#                     filename=file_name, 
#                     key=file_key, 
#                     bucket_name=self.bucket_name, 
#                     replace=True
#                 )
#                 os.remove(file_name)