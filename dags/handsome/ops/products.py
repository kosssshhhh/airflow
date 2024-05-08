# import asyncio
import logging
import requests
from bs4 import BeautifulSoup

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from core.infra.cache.decorator import MongoResponseCache
from ops.handsome_preprocess import HandsomePreprocess
logger = logging.getLogger(__name__)



class FetchProductListFromCategoryOperator(BaseOperator): 
    preprocessor = HandsomePreprocess()
    url = 'https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=20&pageSize={ITEMS_COUNT}&pageNo=1&norOutletGbCd=J&dispCtgNo={small_category_num}&productListLayout=4&theditedYn=N'
    
    max_item_counts: int = 1
    # client = requests
    
    categories_list = ['388','461','5008','2078','503','513','972','5001','971','970','5002','2082','2081','984','983','5007','988','987','986','5006','5003','5004','5005','990','8452','14784','14166','13492','12310','11472','14000','11648','12132','12758','13524','14156','8578','14584','12468','11880','14176','11586','11212','667','1013','5013','1009','1011','1010','5014','1016','5010','1015','5011','2093','2092','1019','1007','1006','5012','1008','5015','5016','12048','12142','8368','10164','8746']
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
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
        logger.info("product_image_urls: ", product_image_urls)
            
        context["task_instance"].xcom_push(key="product_id_list", value=pids)
        context["task_instance"].xcom_push(key="product_list", value=product_list)
        context["task_instance"].xcom_push(key="product_image_urls", value=product_image_urls)
        context["task_instance"].xcom_push(key="product_review_count", value=product_review_count)
    @MongoResponseCache(type='json', key='handsome.product')
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
        

    def _get(self, url, **kwargs):
        # self.headers
        response = requests.get(url, headers=self.headers)
        return response
    

    def _fetch(self, url: str):
        response = self._get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        return soup
    
    
    def _gather(self, xcomData):
        tasks = []

        for product in xcomData:
            url = self.url.format(product_id = product['product_id'])
            # tasks.append(self._parse(self._fetch(url), product['product_id']))
            tasks.append(self.preprocessor.get_product_info(self._fetch(url), product['product_id']))
        return tasks
    
    
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
        
        
    def _fetch(self, url: str):
        return self._get(url).json()

    def _get(self, url, **kwargs):
        response = requests.get(url, headers=self.headers)
        return response
    
    def _gather(self, outputs):
        tasks = []
        
        # pid: key , review_count: value
        for product_id, review_count in outputs.items():
            url = self.url.format(goodsNo=product_id, goodsRevCnt=review_count)
            tasks += self.preprocessor.get_review(self._fetch(url))
        return tasks