import logging
import requests
from bs4 import BeautifulSoup
import json
import re

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

class FetchImageOperator(BaseOperator):
    URL = 'https://www.musinsa.com/app/goods/{product_id}'
    
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
        
        product_image_result = self._gather(xcomData)
        context["task_instance"].xcom_push(key="product_image", value=product_image_result)
        logger.info(f"product_count : {len(product_image_result)}")
        

    
    def _fetch(self, url):
        return self._get(url)
    

    
    # TODO: decorator 추가 
    def _get(self, url):
        response = requests.get(url, headers=self.headers)
        return response
    
    
    
    def _gather(self, xcomData):
        product_image_dict = {}
        
        for product_id in xcomData: 
            
            soup = BeautifulSoup(self._fetch(self.URL.format(product_id=product_id)).text, 'lxml')
            product_json = self._parse(soup)
            
            if product_json is None:
                continue
            
            product_image = self._processing(product_json)
            
            product_image_dict[product_id] = product_image
            
            logger.info(f"product_image: {product_image} is done")
            
            
        return product_image_dict 
    
    
        
    def _processing(self, tasks):   
        image_urls = []
        image_urls.append(f'https://image.msscdn.net{tasks['thumbnailImageUrl']}')
        goodsImages = tasks['goodsImages']
        
        for goodsImage in goodsImages:
            image_urls.append(f'https://image.msscdn.net{goodsImage['imageUrl']}')
            
        return image_urls
        
    
    def _parse(self, soup):
        try:
            info = soup.find_all('script', {'type':'text/javascript'})[15]
        except:
            return
        info = info.string

        pattern = re.compile(r'window\.__MSS__\.product\.state = ({.*?});\s*$', re.DOTALL)
        match = pattern.search(info)
        info = match.group(1)
        
        return json.loads(info)