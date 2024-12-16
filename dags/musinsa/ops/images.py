import logging
import requests
from bs4 import BeautifulSoup
import json
import re
import httpx

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from core.infra.cache.decorator import MongoResponseCache
from musinsa.ops.musinsa_preprocess import MusinsaPreprocess

logger = logging.getLogger(__name__)

class FetchImageOperator(BaseOperator):
    URL = 'https://www.musinsa.com/products/{style_id}'
    preprocessor = MusinsaPreprocess()
    timeout: float = 120.0
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }

    client = httpx.Client(headers=headers)
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_image_urls = self._gather(xcomData, execution_date)
        context["task_instance"].xcom_push(key="style_image_urls", value=style_image_urls)
        logger.info(f"style_count : {len(style_image_urls)}")
        

    
    @MongoResponseCache(db='musinsa', type='html', key='musinsa.image', collection='musinsa.image', date_key='execution_date')
    def _get(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
    
    
    
    def _gather(self, xcomData, execution_date):
        style_image_dict = {}
        
        for style_id in xcomData: 
            url = self.URL.format(style_id=style_id)
            soup = BeautifulSoup(self._get(url=url, execution_date=execution_date), 'lxml')
            style_json = self.preprocessor.parse(soup)
            
            if style_json is None:
                continue
            
            style_image = self.preprocessor.processing_image(style_json)
            
            style_image_dict[style_id] = style_image
            
            
            
        return style_image_dict 
    
    
