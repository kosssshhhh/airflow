import logging
import requests
import numpy as np
import re
import json
from bs4 import BeautifulSoup

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from musinsa.ops.musinsa_preprocess import MusinsaPreprocess
from core.infra.cache.decorator import MongoResponseCache

logger = logging.getLogger(__name__)
MAX_COUNT = 1

# TODO: DB에서 불러오기
# middle_category_list = ['001006', '001004', '001005', '001010', '001002', '001003',
#                         '001001', '001011', '001013', '001008', '002022', '002001',
#                         '002002', '002025', '002017', '002003', '002020', '002019',
#                         '002023', '002018', '002004', '002008', '002007', '002024',
#                         '002009', '002013', '002012', '002016', '002021', '002014',
#                         '002006', '002015', '003002', '003007', '003008', '003004',
#                         '003009', '003005', '003010', '003011', '003006', '002006',
#                         '002007', '002008', '022001', '022002', '022003']

middle_category_list = ['001006', '001004']

class FetchProductListFromCategoryOperator(BaseOperator): 
    preprocessor = MusinsaPreprocess()
    url = 'https://www.musinsa.com/categories/item/{category_number}?d_cat_cd={category_number}&brand=&list_kind=small&sort=sale_high&sub_sort=1d&page={page}&display_cnt=90&exclusive_yn=&sale_goods=&timesale_yn=&ex_soldout=&plusDeliveryYn=&kids=&color=&price1=&price2=&shoeSizeOption=&tags=&campaign_id=&includeKeywords=&measure='
    max_item_counts: int = MAX_COUNT
    middle_category_list = middle_category_list
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    
    def execute(
        self,
        context: Context,
    ):
        # items = asyncio.
        total_product_list = self._gather()
        total_product_id_list = [item[0] for item in total_product_list]
        
        logger.info(f"product_count: {len(total_product_list)}")

        context["task_instance"].xcom_push(key="product_id_list", value=total_product_id_list)
        context["task_instance"].xcom_push(key="product_list", value=total_product_list)

        
    @MongoResponseCache(type='html', key='musinsa.product_list', collection='musinsa.response')
    def _get(self, url, key=None):
        response = requests.get(url, headers=self.headers)
        return response.text
 
    
    def _gather(self):
        total_product_list = [] # 모든 카테고리 4700개 전체 상품 
        
        for middle_category in self.middle_category_list:
            page = 1
            url = self.url.format(category_number=middle_category, page=1)
            soup = BeautifulSoup(self._get(url=url), 'lxml')
            
            flag = 0
            
            total_page = self.preprocessor.get_total_page_counts(soup)
            
            pd_list = []
            
            while flag == 0:
                product_ids = soup.select('ul > li.li_box')
                for product_id in product_ids:
                    pd_list.append([product_id['data-no'], middle_category])
                    
                    if len(pd_list) == self.max_item_counts:
                        flag = 1
                        break   
                
                page += 1
                
                if page > total_page:
                    break
                
                url = self.url.format(category_number=middle_category, page=page)
                soup = BeautifulSoup(self._get(url=url), 'lxml')

            
            pd_list = self.preprocessor.merge_rank_score(pd_list)
            
            total_product_list += pd_list    
            
            logger.info(f"middle_category: {middle_category} is done")
                
        return total_product_list
                
        

class FetchProductOperator(BaseOperator):
    preprocessor = MusinsaPreprocess()
    info_URL = 'https://www.musinsa.com/app/goods/{product_id}'
    stat_URL = 'https://goods-detail.musinsa.com/goods/{product_id}/stat'
    like_URL = 'https://like.musinsa.com/like/api/v2/liketypes/goods/counts'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_list")
        logger.info(f"xcomData : {xcomData}")
        
        product_info_result = self._gather(xcomData)
        context["task_instance"].xcom_push(key="product_info", value=product_info_result)
        
    
    @MongoResponseCache(type='json', key='musinsa.product.like', collection='musinsa.response')
    def _post(self, url, payload, key=None):
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()

    @MongoResponseCache(type='json', key='musinsa.product.stat', collection='musinsa.response')
    def _get_json(self, url, key=None):
        response = requests.get(url, headers=self.headers)
        return response.json()

    @MongoResponseCache(type='html', key='musinsa.product.info', collection='musinsa.response')
    def _get_html(self, url, key=None):
        response = requests.get(url, headers=self.headers)
        return response.text
    
    def _gather(self, xcomData):
        product_list = []
        
        for product_id, middle_category, rank_score in xcomData: 
            
            soup = BeautifulSoup(self._get_html(url=self.info_URL.format(product_id=product_id)), 'lxml')
            like_res = self._post(url=self.like_URL, payload={'relationIds': [product_id]})
            stat_res = self._get_json(url=self.stat_URL.format(product_id=product_id))
            
            product_json = self.preprocessor.parse(soup)
            
            if product_json is None:
                continue
            
            product_info = self.preprocessor.processing_info(product_json)
            product_like = self.preprocessor.processing_like(like_res)
            product_stat = self.preprocessor.processing_stat(stat_res)
            
            merged_data = {**product_info, **product_stat, 'like': product_like, 'rank_score': rank_score, 'middle_category': middle_category}
            
            product_list.append(merged_data)
            
            logger.info(f"product_id: {product_id} is done")
            
            
        return product_list 
        
    

        
