# import asyncio
import logging
import requests
from bs4 import BeautifulSoup
import json

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

from core.infra.cache.decorator import MongoResponseCache
from handsome.ops.handsome_preprocess import HandsomePreprocess
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)



class FetchProductListFromCategoryOperator(BaseOperator): 
    preprocessor = HandsomePreprocess()
    url = 'https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=20&pageSize={ITEMS_COUNT}&pageNo=1&norOutletGbCd=J&dispCtgNo={small_category_num}&productListLayout=4&theditedYn=N'
    
    max_item_counts: int = 100
    
    # categories_list = [388,461,5008,2078,503,513,972,5001,971,970,5002,2082,2081,984,983,5007,988,987,986,5006,5003,5004,5005,990,8452,14784,14166,13492,12310,11472,14000,11648,12132,12758,13524,14156,8578,14584,12468,11880,14176,11586,11212,667,1013,5013,1009,1011,1010,5014,1016,5010,1015,5011,2093,2092,1019,1007,1006,5012,1008,5015,5016,12048,12142,8368,10164,8746]
    # Variable.set('handsome_category_nums', json.dumps(categories_list))
    
    categories_list = Variable.get('handsome_category_nums')
    categories_list = json.loads(categories_list)
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
        logger.info('categories_list: ', self.categories_list)
        logger.info('len: ', len(self.categories_list))

        # items = asyncio.
        product_list = self._gather()
        logger.info("product_list : ", product_list)
        pids = []
        product_image_urls = {}
        product_review_count = {}

        for product in product_list:
            product_id = product['product_id']
            pids.append(product_id)
            url = product.get("url")
            product_image_urls[product_id] = url
            product_review_count[product_id] = product.get("review_count")
            

        logger.info("pids: ",pids)
            
        context["task_instance"].xcom_push(key="product_id_list", value=pids)
        context["task_instance"].xcom_push(key="product_list", value=product_list)
        context["task_instance"].xcom_push(key="product_image_urls", value=product_image_urls)
        context["task_instance"].xcom_push(key="product_review_count", value=product_review_count)
        

    @MongoResponseCache(type='json', key='handsome.product_list', collection='handsome.response')
    def _fetch(self, url: str,key=None):
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    
    def _gather(self):
        tasks = []
        category: str 
        
        for category in self.categories_list:
            url = self.url.format(ITEMS_COUNT=self.max_item_counts, small_category_num=category)
            tasks += self._processing(self._fetch(url=url), category)
            
        return tasks
            
            
    def _processing(self, tasks, category):
        product_list = []
        
        goods_in_page= tasks['payload']['goodsList']
        
        for rank, goods in enumerate(goods_in_page, 1):
            goods_data = self.preprocessor.get_product(goods)
            goods_data['rank_score'] = self.preprocessor.get_rank_score(int(rank), len(goods_in_page))
            goods_data['smallCategory'] = category
            product_list.append(goods_data)
        
        return product_list
         
         
         
         

# 두번째
class FetchProductOperator(BaseOperator):
    preprocessor = HandsomePreprocess()
    url = 'https://pcw.thehandsome.com/ko/PM/productDetail/{product_id}?itmNo=003'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_list")

        logger.info(f"xcomData : {xcomData}")
        
        product_info_result = self._gather(xcomData)
        logger.info(f"result : {product_info_result}")
        context["task_instance"].xcom_push(key="product_info", value=product_info_result)
        
    
    @MongoResponseCache(type='html', key='handsome.product', collection='handsome.response')
    def _get(self, url: str, key=None):
        # self.headers
        response = requests.get(url, headers=self.headers)
        return response.text
    

    def _fetch(self, url: str):
        response_text = self._get(url=url)
        soup = BeautifulSoup(response_text, 'lxml')
        return soup
    
    
    def _gather(self, xcomData):
        tasks = []

        for product in xcomData:
            fixed_price = product['fixed_price']
            discounted_price = product['discounted_price']
            colors = product['colors']
            sizes = product['sizes']
            review_count = product['review_count']
            rank_score = product['rank_score']
            smallCategory = product['smallCategory']
            
            url = self.url.format(product_id = product['product_id'])
            product_info = self.preprocessor.get_product_info(self._fetch(url), product['product_id'])
            
            combined_info = {
            "fixed_price": fixed_price,
            "discounted_price": discounted_price,
            "colors": colors,
            "sizes": sizes,
            "review_count": review_count,
            "rank_score": rank_score,
            "smallCategory": smallCategory
            }
            
            combined_info.update(product_info)
            
            tasks.append(combined_info)

        return tasks
    
    
