import asyncio
import logging
import random
import requests

import httpx
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from core.infra.cache.mongo.decorator import MongoResponseCache
from pendulum.datetime import DateTime

logger = logging.getLogger(__name__)

class FetchProductListFromCategoryOperator(BaseOperator): 
    url = 'https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=20&pageSize={item_counts}&pageNo=1&norOutletGbCd=J&dispCtgNo={small_category_num}&productListLayout=4&theditedYn=N'
    
    item_counts: int = 100
    timeout: float = 120.0
    categories_list = [388,461,5008,2078,503,513,972,5001,971,970,5002,2082,2081,984,983,5007,988,987,986,5006,5003,5004,5005,990,8452,14784,14166,13492,12310,11472,14000,11648,12132,12758,13524,14156,8578,14584,12468,11880,14176,11586,11212,667,1013,5013,1009,1011,1010,5014,1016,5010,1015,5011,2093,2092,1019,1007,1006,5012,1008,5015,5016,12048,12142,8368,10164,8746]
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
        items = asyncio.
        pids = []
        for item in items:
            pids.append(item["productId"])
            
        
        context["task_instance"].xcom_push(key="item_ids", value=pids)
        context["task_instance"].xcom_push(key="items", value=items)
        # context["task_instance"].xcom_push(key="item_image_urls", value='미정')
    
    def _fetch(self, url: str):
        for middle_category_info in 
        response = requests.get(url, headers=headers)
        goods_in_page = _extract(response)
        return 

    def _extract(self, response ):
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        info = soup.string
        goods_in_page = json.loads(info)['payload']['goodsList']