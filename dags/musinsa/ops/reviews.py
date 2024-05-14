import logging
import requests
from bs4 import BeautifulSoup
import time

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from musinsa.ops.musinsa_preprocess import MusinsaReviewPreprocess
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)

class FetchReviewOperator(BaseOperator):
    preprocessor = MusinsaReviewPreprocess()
    URL = 'https://goods.musinsa.com/api/goods/v2/review/{review_type}/list?similarNo=0&sort=up_cnt_desc&selectedSimilarNo={product_id}&page={page_num}&goodsNo={product_id}&rvck=202404150551&_=1713416512533'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        logger.info(f"xcomData : {xcomData}")
        
        product_review_result = self._gather(xcomData)
        
        context["task_instance"].xcom_push(key="product_review", value=product_review_result)
        logger.info(f"product_review_count : {len(product_review_result)}")
    
    
    @MongoResponseCache(type='html', key='musinsa.page.reviews', collection='musinsa.response')
    def _get(self, url, key=None):
        response = requests.get(url, headers=self.headers)
        return response.text
    
    def _gather(self, xcomData):
        product_review_list = []
        review_types = ['style', 'goods', 'photo']
        
        
        for product_id in xcomData:
            for review_type in review_types:
                page_num = 1    
                while True:
                    url = self.URL.format(review_type=review_type, product_id=product_id, page_num=page_num)
                    soup = BeautifulSoup(self._get(url=url), 'lxml')
                    
                    if not soup.select('p.review-profile__name'):
                        break
                    
                    one_page_review = self.preprocessor.parse(soup, product_id, review_type)
                    logger.info(f"one_page_review : {one_page_review}")
                    
                    product_review_list += one_page_review
                    
                    page_num += 1

            # time.sleep(0.5)
        
        logger.info(f"product_review_count : {len(product_review_list)}")
        return product_review_list 
    