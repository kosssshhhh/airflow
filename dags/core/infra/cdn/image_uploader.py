import boto3
import logging
import io
from botocore.exceptions import ClientError
import urllib.request
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 

logger = logging.getLogger(__name__)

class ImageUploader:
    def __init__(self, bucket_name, folder_name, aws_conn_id='aws.s3'):
        self.hook = S3Hook(aws_conn_id)
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.s3_client = self.hook.get_conn()
        self.extra_args = {'ContentType': 'image/webp'}
    
    def upload_images(self, xcomData):
        url_dict = {}

        for key in xcomData:
            url_list = []
            
            for i, url in enumerate(xcomData[key]):
                file_key = f"image/{self.folder_name}/{key}/{i}.webp"
                
                try:
                    self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
                    logger.info(f"File already exists: {file_key}. Skipping upload.")
                    url = f"https://cdn.{self.bucket_name}.com/{file_key}"
                    url_list.append(url)
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.info(f"Uploading file: {file_key}")
                        try:
                            with urllib.request.urlopen(url) as tmp:
                                data = tmp.read()
                                self.s3_client.upload_fileobj(
                                    io.BytesIO(data),
                                    self.bucket_name,
                                    file_key,
                                    ExtraArgs=self.extra_args
                                )
                                url = f"https://cdn.{self.bucket_name}.com/{file_key}"
                                url_list.append(url)
                        except Exception as upload_error:
                            logger.error(f"Failed to upload {file_key}: {upload_error}")

            url_dict[key] = url_list
            
        return url_dict
