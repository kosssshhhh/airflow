import logging
import requests
import numpy as np

import httpx
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from wconcept.ops.wconcept_preprocess import WconceptPreprocess
from core.infra.cache.decorator import MongoResponseCache

from pendulum.datetime import DateTime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class FetchImageOperator(BaseOperator):
    preprocessor = WconceptPreprocess()
    url = 'https://www.wconcept.co.kr/Product/{style_id}?rccode=pc_topseller'
    timeout: float = 120.0
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
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
        logger.info(f"len(style_image_url) : {len(style_image_urls)}")
        context["task_instance"].xcom_push(key="style_image_urls", value=style_image_urls)

    
    @MongoResponseCache(db='wconcept', type='html', key='wconcept.image', collection='wconcept.image', date_key='execution_date')
    def _get(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
 
    
    def _gather(self, xcomData, execution_date):
        styles_image_dict = {}
        
        for style_id in xcomData:
            url = self.url.format(style_id=style_id)
            soup = BeautifulSoup(self._get(url=url, execution_date=execution_date), 'lxml')
    
            try:
                imageUrls = self.preprocessor.parse_image(soup)
                styles_image_dict[style_id] = imageUrls

            except:
                styles_image_dict[style_id] = []
                
        return styles_image_dict

        
