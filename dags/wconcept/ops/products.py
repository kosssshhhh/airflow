import logging
import requests
import numpy as np
from bs4 import BeautifulSoup
import json

# import httpx
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from pendulum.datetime import DateTime
from wconcept.ops.wconcept_preprocess import WconceptPreprocess
from core.infra.cache.decorator import MongoResponseCache
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)
MAX_COUNT = 100

# TODO: DB에서 불러오기
# middle_category_list = ['10101201', '10101202', '10101203', '10101204', '10101205',
#                         '10101206', '10101207', '10101208', '10101209', '10101210',
#                         '10101211', '10101212']

class FetchProductListFromCategoryOperator(BaseOperator): 
    preprocessor = WconceptPreprocess()
    url = 'https://api-display.wconcept.co.kr/display/api/v2/best/products'
    max_item_counts: int = MAX_COUNT
    # middle_category_list = middle_category_list
    middle_category_list = Variable.get('wconcept_category_nums')
    middle_category_list = json.loads(middle_category_list)
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
}
    
    def execute(
        self,
        context: Context,
    ):
        # items = asyncio.
        total_product_list, total_product_info_list = self._gather()

        context["task_instance"].xcom_push(key="product_id_list", value=total_product_list)
        context["task_instance"].xcom_push(key="product_list", value=total_product_info_list)

    
    # def _fetch(self, url, payload):
    #     return self._post(url, payload)
        

    @MongoResponseCache(type='json', key='wconcept.product_list', collection='wconcept.response')
    def _post(self, url, payload, key=None):
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()
 
    
    def _gather(self):
        total_product_list = []
        total_product_info_list = []
        
        for gender in ['men', 'women']:
            for middle_category in self.middle_category_list:
                url = self.url
                payload = self.preprocessor.get_payload(self.max_item_counts, middle_category, gender)
                
                response = self._post(url=url, payload=payload)
                product_list, product_info_list = self.preprocessor.get_product_and_product_info_list(response)
                
                total_product_list += product_list
                total_product_info_list += product_info_list
                
        return total_product_list, total_product_info_list
                
        

class FetchProductOperator(BaseOperator):
    preprocessor = WconceptPreprocess()
    url = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
    }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_list")
        logger.info(f"xcomData : {xcomData}")
        
        product_info_result = self._gather(xcomData)
        context["task_instance"].xcom_push(key="product_info", value=product_info_result)

    
    # def _fetch(self, url, payload):
    #     return self._post(url=url, payload)[0]
    

    @MongoResponseCache(type='json', key='wconcept.product', collection='wconcept.response')
    def _post(self, url, payload, key=None):
        response = requests.post(url, headers=self.headers, data=payload)
        return response.json()
 
    
    def _gather(self, xcomData):
        product_list = []
        
        for product_id, product_fp, ranking, middle_item_count in xcomData: 
            payload = {"itemcds": int(product_id)}

            product = self.preprocessor.get_product(self._post(url=self.url, payload=payload)[0], product_fp)
            product['rank_score'] = self.preprocessor.get_rank_score(int(ranking), int(middle_item_count))
            product_list.append(product)
            
        return product_list 
                
    
