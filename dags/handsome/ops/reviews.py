import logging
import requests
import json
import httpx

from airflow.models.variable import Variable
from airflow.models.baseoperator import BaseOperator
from handsome.ops.handsome_preprocess import HandsomePreprocess
from airflow.utils.context import Context
from airflow.models.taskinstance import TaskInstance
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)

class FetchReviewOperator(BaseOperator):
    preprocessor = HandsomePreprocess()
    url = 'https://www.thehandsome.com/api/goods/1/ko/goods/{goodsNo}/reviews?sortTypeCd=latest&revGbCd=&pageSize={goodsRevCnt}&pageNo=1'
    timeout: float = 120.0
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
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
        outputs = task_instance.xcom_pull(task_ids="fetch.styles", key="style_review_count")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        result = self._gather(outputs, execution_date)
        logger.info(f"수집된 리뷰 수 : {len(result)} ")
        context["task_instance"].xcom_push(key="style_reviews", value=result)
        
    @MongoResponseCache(db='handsome', type='json', key='handsome.review', collection='handsome.review', date_key='execution_date')
    def _fetch(self, url: str, execution_date=None, key=None):
        return self._get(url).json()
    
    def _get(self, url, **kwargs):
        response = self.client.get(url, timeout=self.timeout)
        return response
    
    def _gather(self, outputs, execution_date):
        tasks = []
        
        # pid: key , review_count: value
        for style_id, review_count in outputs.items():
            url = self.url.format(goodsNo=style_id, goodsRevCnt=review_count)
            tasks += self.preprocessor.get_review(self._fetch(url=url, execution_date=execution_date))
        return tasks