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
from core.infra.cache.decorator import MongoResponseCache, MongoAsyncResponseCache
from airflow.models.variable import Variable
from requests.auth import HTTPProxyAuth

logger = logging.getLogger(__name__)

class FetchStyleListFromBrandOperator(BaseOperator):
    url =  'https://api.musinsa.com/api2/dp/v1/plp/goods?brand={brand_name}&gf=A&sortCode=POPULAR&page={page_num}&size=100&caller=BRAND' 
    timeout: float = 120.0
    
    brands_list = Variable.get('musinsa_brands')
    brands_list = json.loads(brands_list)
    
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
        total_style_id_list = self._gather(execution_date)
        logger.info(f"style_count: {len(total_style_id_list)}")
        context["task_instance"].xcom_push(key="style_id_list", value=total_style_id_list)
        

    @MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.brand.style_list', collection='musinsa.style_ids', date_key='execution_date')
    def _get(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.json() 
    
    def _gather(self, execution_date):
        total_style_list = []
        
        for brand in self.brands_list:
            page_num = 1
            
            while True:
                url = self.url.format(brand_name=brand, page_num=page_num)
                response = self._get(url=url, execution_date=execution_date)
                items = response['data']['list']
                if len(items) == 0:
                    break
                
                partial_style_list = [item['goodsNo'] for item in items]
                total_style_list += partial_style_list
                
                page_num += 1
                
            logger.info(f"brand: {brand} is done")
            
        return total_style_list
    
    
class FetchStyleListAsyncFromBrandOperator(BaseOperator):
    url = 'https://api.musinsa.com/api2/dp/v1/plp/goods?brand={brand_name}&gf=A&sortCode=POPULAR&page={page_num}&size=300&caller=BRAND' 
    timeout: float = 120.0
    
    brands_list = Variable.get('musinsa_brands')
    brands_list = json.loads(brands_list)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    
    username = Variable.get("username")
    password = Variable.get("password")
    proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
    async_client = httpx.AsyncClient(proxies=proxy, headers=headers)

    async def execute(self, context: Context):
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        total_style_id_list = await self._gather(execution_date)
        logger.info(f"수집된 스타일 수: {len(total_style_id_list)}")
        context["task_instance"].xcom_push(key="style_id_list", value=total_style_id_list)

    @MongoAsyncResponseCache(db='musinsa_brand', type='json', key='musinsa.brand.style_list', collection='musinsa.style_ids', date_key='execution_date')
    async def _get(self, url, execution_date=None, key=None):
        response = await self.async_client.get(url, timeout=self.timeout)
        return response.json()

    async def _gather(self, execution_date):
        tasks = []
        for brand in self.brands_list:
            page_num = 1
            while True:
                url = self.url.format(brand_name=brand, page_num=page_num)
                tasks.append(self._get(url=url, execution_date=execution_date))
                # 응답에 따라 페이지 번호를 결정하는 로직 필요
        responses = await asyncio.gather(*tasks)
        total_style_list = [item for response in responses for item in response['data']['list']]
        return total_style_list

                

class FetchStyleBrandOperator(BaseOperator):
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
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_info_result = self._gather(xcomData, execution_date)
        logger.info(f"수집된 상품 수 : {len(style_info_result)} ")
        context["task_instance"].xcom_push(key="style_info", value=style_info_result)
        
    
    @MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.style.like', collection='musinsa.style_info', payload=True, date_key='execution_date')
    def _post(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, json=payload, timeout=self.timeout)
        return response.json()

    @MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.style.stat', collection='musinsa.style_info', date_key='execution_date')
    def _get_json(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.json()

    @MongoResponseCache(db='musinsa_brand', type='html', key='musinsa.style.info', collection='musinsa.style_info', date_key='execution_date')
    def _get_html(self, url, execution_date=None, key=None):
        response = self.client.get(url, timeout=self.timeout)
        return response.text
    
    def _gather(self, xcomData, execution_date):
        style_list = []
        
        for style_id in xcomData: 
            
            soup = BeautifulSoup(self._get_html(url=self.info_URL.format(style_id=style_id), execution_date=execution_date), 'lxml')
            like_res = self._post(url=self.like_URL, execution_date=execution_date, payload={'relationIds': [style_id]})
            stat_res = self._get_json(url=self.stat_URL.format(style_id=style_id), execution_date=execution_date)
            
            style_json = self.preprocessor.parse(soup)
            
            if style_json is None:
                continue
            
            style_info = self.preprocessor.processing_info(style_json)
            style_like = self.preprocessor.processing_like(like_res)
            style_stat = self.preprocessor.processing_stat(stat_res)
            
            merged_data = {**style_info, **style_stat, 'like': style_like}
            
            style_list.append(merged_data)
            
            
        return style_list
    
    
    
class FetchStyleAsyncBrandOperator(BaseOperator):
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
    async_client = httpx.AsyncClient(proxies=proxy, headers=headers)

    async def execute(self, context: Context):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_info_result = await self._gather(xcomData, execution_date)
        logger.info(f"수집된 상품 정보 수 : {len(style_info_result)} ")
        context["task_instance"].xcom_push(key="style_info", value=style_info_result)
        
    @MongoAsyncResponseCache(db='musinsa_brand', type='json', key='musinsa.style.like', collection='musinsa.style_info', payload=True, date_key='execution_date')
    async def _post(self, url, payload, execution_date=None, key=None):
        response = await self.async_client.post(url, json=payload, timeout=self.timeout)
        return response.json()

    @MongoAsyncResponseCache(db='musinsa_brand', type='html', key='musinsa.style.info', collection='musinsa.style_info', date_key='execution_date')
    async def _get_html(self, url, execution_date=None, key=None):
        response = await self.async_client.get(url, timeout=self.timeout)
        return response.text

    async def _gather(self, xcomData, execution_date):
        tasks = []
        for style_id in xcomData:
            tasks.append(self._process_style_info(style_id, execution_date))
        style_infos = await asyncio.gather(*tasks)
        return style_infos

    async def _process_style_info(self, style_id, execution_date):
        soup = BeautifulSoup(await self._get_html(url=self.info_URL.format(style_id=style_id), execution_date=execution_date), 'lxml')
        like_res = await self._post(url=self.like_URL, payload={'relationIds': [style_id]}, execution_date=execution_date)
        stat_res = await self._get_json(url=self.stat_URL.format(style_id=style_id), execution_date=execution_date)

        style_json = self.preprocessor.parse(soup)
        if style_json is None:
            return None
        
        style_info = self.preprocessor.processing_info(style_json)
        style_like = self.preprocessor.processing_like(like_res)
        style_stat = self.preprocessor.processing_stat(stat_res)

        return {**style_info, **style_stat, 'like': style_like}
        
            


    
    