import logging
import requests
import numpy as np

# import httpx
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
    url = 'https://www.wconcept.co.kr/Product/{product_id}?rccode=pc_topseller'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
    }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        logger.info(f"xcomData : {xcomData}")
        
        product_image_result = self._gather(xcomData)
        logger.info(f"result : {product_image_result}")
        context["task_instance"].xcom_push(key="product_imageURL", value=product_image_result)

    
    @MongoResponseCache(type='html', key='wconcept.image', collection='wconcept.response')
    def _get(self, url, key=None):
        response = requests.get(url, headers=self.headers)
        return response.text
 
    
    def _gather(self, xcomData):
        products_image_dict = {}
        
        for product_id in xcomData:
            url = self.url.format(product_id=product_id)
            soup = BeautifulSoup(self._get(url=url), 'lxml')
    
            try:
                imageUrls = self.preprocessor.parse_image(soup)
                products_image_dict[product_id] = imageUrls

            except:
                products_image_dict[product_id] = []
                
        return products_image_dict

        
