import logging
import requests
import numpy as np
import re
import json
from bs4 import BeautifulSoup
import httpx

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from musinsa.ops.musinsa_preprocess import MusinsaPreprocess
from core.infra.cache.decorator import MongoResponseCache
from airflow.models.variable import Variable
from requests.auth import HTTPProxyAuth

logger = logging.getLogger(__name__)
MAX_COUNT = int(Variable.get("musinsa_max_count"))

class FetchStyleListFromCategoryOperator(BaseOperator): 
    preprocessor = MusinsaPreprocess()
    # url = 'https://display.musinsa.com/display/api/v2/categories/ITEM/goods?siteKindId=musinsa&sex=A&sortCode=1d&categoryCode={category_number}&size={item_counts}&page=1'
    url = 'https://api.musinsa.com/api2/dp/v1/plp/goods?gf=A&sortCode=SALE_ONE_DAY_COUNT&category={category_number}&caller=CATEGORY&page=1&size={item_counts}'
    max_item_counts: int = MAX_COUNT
    timeout: float = 120.0

    middle_category_list = Variable.get('musinsa_category_nums')
    middle_category_list = json.loads(middle_category_list)
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
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        total_style_list = self._gather(execution_date)
        total_style_id_list = [item[0] for item in total_style_list]
        
        logger.info(f"style_count: {len(total_style_list)}")

        context["task_instance"].xcom_push(key="style_id_list", value=total_style_id_list)
        context["task_instance"].xcom_push(key="style_list", value=total_style_list)

        
    @MongoResponseCache(db='musinsa', type='json', key='musinsa.style_list', collection='musinsa.topK_styles', date_key='execution_date')
    def _get(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.json()
 
    
    def _gather(self, execution_date):
        total_style_list = [] # 모든 카테고리 4700개 전체 상품 
        
        for middle_category in self.middle_category_list:
            
            url = self.url.format(category_number=middle_category, item_counts=self.max_item_counts)
            response = self._get(url=url, execution_date=execution_date)
            styles = response['data']['list']
            pd_list = []
            for style in styles:
                style_num = style['goodsNo']
                pd_list.append([style_num, middle_category])
            
            pd_list = self.preprocessor.merge_rank_score(pd_list)
            
            total_style_list += pd_list    
            
            logger.info(f"middle_category: {middle_category} is done")
                
        return total_style_list
                
        

class FetchStyleOperator(BaseOperator):
    preprocessor = MusinsaPreprocess()
    info_URL = 'https://www.musinsa.com/products/{style_id}'
    stat_URL = 'https://goods-detail.musinsa.com/goods/{style_id}/stat'
    like_URL = 'https://like.musinsa.com/like/api/v2/liketypes/goods/counts'
    timeout: float = 60.0
    
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
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_info_result = self._gather(xcomData, execution_date)
        logger.info(f"수집된 상품 수 : {len(style_info_result)} ")
        context["task_instance"].xcom_push(key="style_info", value=style_info_result)
        
    
    @MongoResponseCache(db='musinsa', type='json', key='musinsa.style.like', collection='musinsa.style_info', payload=True, date_key='execution_date')
    def _post(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, json=payload, timeout=self.timeout)
        return response.json()

    @MongoResponseCache(db='musinsa', type='json', key='musinsa.style.stat', collection='musinsa.style_info', date_key='execution_date')
    def _get_json(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.json()

    @MongoResponseCache(db='musinsa', type='html', key='musinsa.style.info', collection='musinsa.style_info', date_key='execution_date')
    def _get_html(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
    
    def _gather(self, xcomData, execution_date):
        style_list = []
        
        for style_id, middle_category, rank_score in xcomData: 
            
            soup = BeautifulSoup(self._get_html(url=self.info_URL.format(style_id=style_id), execution_date=execution_date), 'lxml')
            like_res = self._post(url=self.like_URL, execution_date=execution_date, payload={'relationIds': [style_id]})
            stat_res = self._get_json(url=self.stat_URL.format(style_id=style_id), execution_date=execution_date)
            
            style_json = self.preprocessor.parse(soup)
            
            if style_json is None:
                continue
            
            style_info = self.preprocessor.processing_info(style_json)
            style_like = self.preprocessor.processing_like(like_res)
            style_stat = self.preprocessor.processing_stat(stat_res)
            
            merged_data = {**style_info, **style_stat, 'like': style_like, 'rank_score': rank_score, 'middle_category': middle_category}
            
            style_list.append(merged_data)
            
            
        return style_list 
        
    

        
