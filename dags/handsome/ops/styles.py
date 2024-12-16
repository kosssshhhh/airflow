# import asyncio
import logging
import requests
from bs4 import BeautifulSoup
import json
import httpx
import pendulum
import pytz

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

from core.infra.cache.decorator import MongoResponseCache
from handsome.ops.handsome_preprocess import HandsomePreprocess
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)



class FetchStyleListFromCategoryOperator(BaseOperator): 
    preprocessor = HandsomePreprocess()
    url = 'https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=20&pageSize={ITEMS_COUNT}&pageNo=1&norOutletGbCd=J&dispCtgNo={small_category_num}&productListLayout=4&theditedYn=N'

    max_item_counts = int(Variable.get("handsome_max_count"))
    timeout: float = 120.0
    
    categories_list = Variable.get('handsome_category_nums')
    categories_list = json.loads(categories_list)
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }


    client = httpx.Client(headers=headers)
    
    def execute(
        self,
        context: Context,
    ):
        logger.info('category_list_len: ', len(self.categories_list))
        execution_date = context['execution_date'].strftime('%Y-%m-%d')

        style_list = self._gather(execution_date)
        pids = []
        style_image_urls = {}
        style_review_count = {}

        for style in style_list:
            style_id = style['style_id']
            pids.append(style_id)
            url = style.get("url")
            style_image_urls[style_id] = url
            style_review_count[style_id] = style.get("review_count")
            

        logger.info(f"수집된 상품 id 수 : {len(pids)} ")
            
        context["task_instance"].xcom_push(key="style_id_list", value=pids)
        context["task_instance"].xcom_push(key="style_list", value=style_list)
        context["task_instance"].xcom_push(key="style_image_urls", value=style_image_urls)
        context["task_instance"].xcom_push(key="style_review_count", value=style_review_count)
        

    @MongoResponseCache(db='handsome', type='json', key='handsome.style_list', collection='handsome.topK_styles', date_key='execution_date')
    def _fetch(self, url: str, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.json()
    
    
    def _gather(self, execution_date):
        tasks = []
        category: str 
        
        for category in self.categories_list:
            url = self.url.format(ITEMS_COUNT=self.max_item_counts, small_category_num=category)
            tasks += self._processing(self._fetch(url=url, execution_date=execution_date), category)
            
        return tasks
            
            
    def _processing(self, tasks, category):
        style_list = []
        
        goods_in_page= tasks['payload']['goodsList']
        
        for rank, goods in enumerate(goods_in_page, 1):
            goods_data = self.preprocessor.get_style(goods)
            goods_data['rank_score'] = self.preprocessor.get_rank_score(int(rank), len(goods_in_page))
            goods_data['smallCategory'] = category
            style_list.append(goods_data)
        
        return style_list
         
         
         
         

# 두번째
class FetchStyleOperator(BaseOperator):
    preprocessor = HandsomePreprocess()
    url = 'https://pcw.thehandsome.com/ko/PM/productDetail/{style_id}?itmNo=003'
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
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_list")

        
        style_info_result = self._gather(xcomData, execution_date)
        logger.info(f"수집된 상품 수 : {len(style_info_result)} ")
        context["task_instance"].xcom_push(key="style_info", value=style_info_result)
        
    
    @MongoResponseCache(db='handsome', type='html', key='handsome.style', collection='handsome.style_info', date_key='execution_date')
    def _get(self, url: str, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
    

    def _fetch(self, url: str, execution_date):
        response_text = self._get(url=url, execution_date=execution_date)
        soup = BeautifulSoup(response_text, 'lxml')
        return soup
    
    
    def _gather(self, xcomData, execution_date):
        tasks = []

        for style in xcomData:
            fixed_price = style['fixed_price']
            style_name = style['style_name']
            discounted_price = style['discounted_price']
            colors = style['colors']
            sizes = style['sizes']
            review_count = style['review_count']
            rank_score = style['rank_score']
            smallCategory = style['smallCategory']
            
            url = self.url.format(style_id = style['style_id'])
            
            try:
                style_info = self.preprocessor.get_style_info(self._fetch(url, execution_date), style['style_id'])
            except Exception as e:
                logger.info(f"{style['style_id']} 수집 실패. 에러 메시지: {str(e)}")
                continue
            
            combined_info = {
            "style_name": style_name,
            "fixed_price": fixed_price,
            "discounted_price": discounted_price,
            "colors": colors,
            "sizes": sizes,
            "review_count": review_count,
            "rank_score": rank_score,
            "smallCategory": smallCategory
            }
            
            combined_info.update(style_info)
            
            tasks.append(combined_info)

        return tasks
    
    
