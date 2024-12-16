import logging
import requests
from bs4 import BeautifulSoup
import time
import httpx

from airflow.models.variable import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from musinsa.ops.musinsa_preprocess import MusinsaReviewPreprocess
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)

class FetchReviewOperator(BaseOperator):
    preprocessor = MusinsaReviewPreprocess()
    URL = 'https://goods.musinsa.com/api/goods/v2/review-list-html?goodsNo={style_id}&sex=&myFilter=false&type={review_type}&bodyFilterAvailable=Y&similarNo=0&selectedSimilarNo=0&selectedSimilarName=&minHeight=0&maxHeight=0&minWeight=0&maxWeight=0&minHeightList=&maxHeightList=&minWeightList=&maxWeightList=&skinType=&skinTone=&skinWorry=&reviewSex=&optionCnt=2&option1=&option2=&option1List=&option2List=&keyword=&page={page_num}&sort=new'
    timeout: float = 120.0

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
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')

        style_review_result = self._gather(xcomData, execution_date)
        
        context["task_instance"].xcom_push(key="style_review", value=style_review_result)
        logger.info(f"style_review_count : {len(style_review_result)}")
    
    
    @MongoResponseCache(db='musinsa', type='html', key='musinsa.page.reviews', collection='musinsa.review', date_key='execution_date')
    def _get(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
    
    def _gather(self, xcomData, execution_date):
        style_review_list = []
        review_types = ['style', 'goods', 'photo']
        
        
        for style_id in xcomData:
            for review_type in review_types: 
                for page_num in range(1,4):
                    url = self.URL.format(review_type=review_type, style_id=style_id, page_num=page_num)
                    soup = BeautifulSoup(self._get(url=url, execution_date=execution_date), 'lxml')
                    
                    if not soup.select('p.review-profile__name'):
                        break
                    
                    one_page_review = self.preprocessor.parse(soup, style_id, review_type)
                    
                    style_review_list += one_page_review

        
        logger.info(f"style_review_count : {len(style_review_list)}")
        return style_review_list 
    