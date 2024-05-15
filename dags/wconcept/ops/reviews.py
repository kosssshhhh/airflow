import logging
import requests
import numpy as np
import re
from bs4 import BeautifulSoup

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from wconcept.ops.wconcept_preprocess import WconceptReviewPreprocess
from core.infra.cache.decorator import MongoResponseCache


logger = logging.getLogger(__name__)


class FetchReviewOperator(BaseOperator):
    preprocessor = WconceptReviewPreprocess()
# TODO: FetchProductImageOperator
    payload_URL = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    review_URL = "https://www.wconcept.co.kr/Ajax/ProductReViewList"
    
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
        
        products_reviews_list = self._gather(xcomData)
        logger.info(f"result : {products_reviews_list}")
        context["task_instance"].xcom_push(key="product_review", value=products_reviews_list)


    @MongoResponseCache(type='json', key='wconcept.review.payload', collection='wconcept.response')
    def _post_json(self, url, payload, key=None):
        response = requests.post(url, headers=self.headers, data=payload)

        return response.json()


    @MongoResponseCache(type='html', key='wconcept.review', collection='wconcept.response')
    def _post_html(self, url, payload, key=None):
        response = requests.post(url, headers=self.headers, data=payload)

        return response.text
    
 
    
    def _gather(self, xcomData):
        products_reviews_list = []
        payloads_list = []
        for product_id in xcomData: 
            payload = {"itemcds": product_id}
            
            fetch_data = self._post_json(url=self.payload_URL, payload=payload)[0]
            
            request_payload = [fetch_data['itemCd'], fetch_data['category'][0]['mediumCd'], fetch_data['category'][0]['categoryCd'], fetch_data['itemTypeCd']]
            
            payloads_list.append(request_payload)
        
        for request_payload in payloads_list:
            product_reviews_list =[]
            page = 1
            
            while True:
                payload = {
                    'itemcd': request_payload[0],
                    'pageIndex': page,
                    'order': 1,
                    'IsPrdCombinOpt': 'N',
                    'mediumcd': request_payload[1],
                    'categorycd': request_payload[2],
                    'itemtypecd': request_payload[3]
                }
                
                reviews_in_page = BeautifulSoup(self._post_html(url=self.review_URL, payload=payload), 'lxml')
                
                page_review_counts = len(reviews_in_page.select('p.pdt_review_text'))
                
                if page_review_counts == 0:
                    break
                
                for page_count in range(page_review_counts):
                    review = self.preprocessor.get_review_data(page_count, reviews_in_page, request_payload[0])
                    product_reviews_list.append(review)
                
                page += 1
            
            if len(product_reviews_list) == 0:
                continue
            else:
                products_reviews_list += product_reviews_list
                
        return products_reviews_list
                

    