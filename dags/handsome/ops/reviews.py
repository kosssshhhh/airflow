import logging
import requests
import json

from airflow.models.baseoperator import BaseOperator
from handsome.ops.handsome_preprocess import HandsomePreprocess
from airflow.utils.context import Context
from airflow.models.taskinstance import TaskInstance
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)

class FetchReviewOperator(BaseOperator):
    preprocessor = HandsomePreprocess()
    url = 'https://www.thehandsome.com/api/goods/1/ko/goods/{goodsNo}/reviews?sortTypeCd=latest&revGbCd=&pageSize={goodsRevCnt}&pageNo=1'
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        outputs = task_instance.xcom_pull(task_ids="fetch.products", key="product_review_count")
        logger.info(f"outputs : {outputs}")
        
        result = self._gather(outputs)
        logger.info(f"result : {result}")
        context["task_instance"].xcom_push(key="product_reviews", value=result)
        
    @MongoResponseCache(type='json', key='handsome.review')
    def _fetch(self, url: str, key=None):
        return self._get(url).json()

    def _get(self, url, **kwargs):
        response = requests.get(url, headers=self.headers)
        return response
    
    def _gather(self, outputs):
        tasks = []
        
        # pid: key , review_count: value
        for product_id, review_count in outputs.items():
            url = self.url.format(goodsNo=product_id, goodsRevCnt=review_count)
            tasks += self.preprocessor.get_review(self._fetch(url=url))
        return tasks