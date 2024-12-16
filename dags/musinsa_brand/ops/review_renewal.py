import logging
import requests
from bs4 import BeautifulSoup
import time
import httpx

from airflow.models.variable import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from musinsa.ops.musinsa_preprocess import MusinsaReviewPreprocess2
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)
MAX_COUNT = int(Variable.get("musinsa_review_max_count"))

class FetchReviewOperator(BaseOperator):
    preprocessor = MusinsaReviewPreprocess2()
    URL = 'https://goods.musinsa.com/api2/review/v1/view/list?page=0&pageSize={review_count}&goodsNo={style_id}&sort=up_cnt_desc&myFilter=false&selectedSimilarNo={style_id}'
    timeout: float = 120.0
    max_review_counts: int = MAX_COUNT
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
    
    username = Variable.get("username")
    password = Variable.get("password")
    proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
    client = httpx.Client(proxies=proxy, headers=headers)
    
    def execute(
        self,
        context: Context,    
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids='fetch.styles', key='style_id_list')
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_review_result = self._gather(xcomData, execution_date)
        
        context['task_instance'].xcom_push(key='style_review', value=style_review_result)
        logger.info(f"style_review_count : {len(style_review_result)}")
    
    @MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.page.reviews', collection='musinsa.review', date_key='execution_date')
    def _fetch(self, url: str, execution_date=None, key=None):
        return self._get(url).json()
       
    def _get(self, url, **kwargs):
        response = self.client.get(url, timeout=self.timeout)
        return response
    
    def _gather(self, xcomData, execution_date):
        style_review_list = []
        
        for style_id in xcomData:
            url = self.URL.format(style_id=style_id, review_count=self.max_review_counts)
            style_review_list += self.preprocessor.get_review(self._fetch(url=url, execution_date=execution_date), style_id)
            
        return style_review_list
            
        
    