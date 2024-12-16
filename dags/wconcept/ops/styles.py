import logging
import requests
import numpy as np
from bs4 import BeautifulSoup
import json
import httpx
import asyncio

from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from pendulum.datetime import DateTime
from wconcept.ops.wconcept_preprocess import WconceptPreprocess
from core.infra.cache.decorator import MongoResponseCache, MongoAsyncResponseCache
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)
MAX_COUNT = int(Variable.get("wconcept_max_count"))

class FetchStyleListFromCategoryOperator(BaseOperator): 
    preprocessor = WconceptPreprocess()
    url = 'https://api-display.wconcept.co.kr/display/api/v2/best/products'
    max_item_counts: int =  MAX_COUNT
    timeout: float = 60.0
    username = Variable.get("username")
    password = Variable.get("password")

    middle_category_list = Variable.get('wconcept_category_nums')
    middle_category_list = json.loads(middle_category_list)
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
    }


    proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
    client = httpx.Client(proxies=proxy, headers=headers)
    timeout: float = 120.0
    
    
    def execute(
        self,
        context: Context,
    ):

        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        total_style_list, total_style_info_list = self._gather(execution_date)
        logger.info(f"수집된 상품 id 수 : {len(total_style_info_list)} ")
        

        context["task_instance"].xcom_push(key="style_id_list", value=total_style_list)
        context["task_instance"].xcom_push(key="style_list", value=total_style_info_list)
        

    @MongoResponseCache(db='wconcept', type='json', key='wconcept.style_list', collection='wconcept.topK_styles', payload=True, date_key='execution_date')
    def _post(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, json=payload, timeout=self.timeout)
        return response.json()
 
    
    def _gather(self, execution_date):
        total_style_list = []
        total_style_info_list = []
        
        for gender in ['men', 'women']:
            for middle_category in self.middle_category_list:
                url = self.url
                payload = self.preprocessor.get_payload(self.max_item_counts, middle_category, gender)
                
                response = self._post(url=url, payload=payload, execution_date=execution_date)
                style_list, style_info_list = self.preprocessor.get_style_and_style_info_list(response)
                
                total_style_list += style_list
                total_style_info_list += style_info_list
                
        return total_style_list, total_style_info_list
    
    
class FetchStyleListAsyncFromCategoryOperator(BaseOperator):
    preprocessor = WconceptPreprocess()
    url = 'https://api-display.wconcept.co.kr/display/api/v2/best/products'
    max_item_counts: int =  MAX_COUNT
    timeout: float = 60.0
    username = Variable.get("username")
    password = Variable.get("password")

    middle_category_list = Variable.get('wconcept_category_nums')
    middle_category_list = json.loads(middle_category_list)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
    }

    proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
    async_client = httpx.AsyncClient(proxies=proxy, headers=headers)
    
    async def execute(self, context: Context):
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        total_style_list, total_style_info_list = await self._gather(execution_date)
        logger.info(f"수집된 상품 id 수 : {len(total_style_info_list)} ")

        context["task_instance"].xcom_push(key="style_id_list", value=total_style_list)
        context["task_instance"].xcom_push(key="style_list", value=total_style_info_list)
        
    async def _gather(self, execution_date):
        tasks = []
        for gender in ['men', 'women']:
            for middle_category in self.middle_category_list:
                payload = self.preprocessor.get_payload(self.max_item_counts, middle_category, gender)
                tasks.append(self._post(self.url, payload, execution_date))

        responses = await asyncio.gather(*tasks)
        total_style_list = []
        total_style_info_list = []
        
        for response in responses:
            style_list, style_info_list = self.preprocessor.get_style_and_style_info_list(response)
            total_style_list += style_list
            total_style_info_list += style_info_list

        return total_style_list, total_style_info_list

    async def _post(self, url, payload, execution_date):
        response = await self.async_client.post(url, json=payload, timeout=self.timeout)
        return response.json()
    
                
        

class FetchStyleOperator(BaseOperator):
    preprocessor = WconceptPreprocess()
    url = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    timeout: float = 120.0
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
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
    

    @MongoResponseCache(db='wconcept', type='json', key='wconcept.style', collection='wconcept.style_info', payload=True, date_key='execution_date')
    def _post(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, data=payload, timeout=self.timeout)
        return response.json()
 
    
    def _gather(self, xcomData, execution_date):
        style_list = []
        
        for style_id, style_fp, ranking, middle_item_count in xcomData: 
            payload = {"itemcds": int(style_id)}

            try:
                style = self.preprocessor.get_style(self._post(url=self.url, payload=payload, execution_date=execution_date)[0], style_fp)
            except:
                logger.info(f"수집 실패: {style_id} ")
                continue
            style['rank_score'] = self.preprocessor.get_rank_score(int(ranking), int(middle_item_count))
            style_list.append(style)
            
        return style_list 
    
    
    
    
class FetchStyleAsyncOperator(BaseOperator):
    preprocessor = WconceptPreprocess()
    url = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    timeout: float = 120.0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
    }

    username = Variable.get("username")
    password = Variable.get("password")
    proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
    async_client = httpx.AsyncClient(proxies=proxy, headers=headers)

    async def execute(self, context: Context):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.styles", key="style_list")
        execution_date = context['execution_date'].strftime('%Y-%m-%d')

        style_info_result = await self._gather(xcomData, execution_date)
        logger.info(f"수집된 상품 수 : {len(style_info_result)}")
        context["task_instance"].xcom_push(key="style_info", value=style_info_result)

    @MongoAsyncResponseCache(db='wconcept', type='json', key='wconcept.style', collection='wconcept.style_info', payload=True, date_key='execution_date')
    async def _post(self, url, payload, execution_date=None, key=None):
        response = await self.async_client.post(url, data=payload, timeout=self.timeout)
        return response.json()

    async def _gather(self, xcomData, execution_date):
        style_list = []

        # 여러 개의 스타일을 비동기적으로 가져오기 위한 작업(task) 생성
        tasks = []
        for style_id, style_fp, ranking, middle_item_count in xcomData:
            payload = {"itemcds": int(style_id)}
            tasks.append(self._process_style(style_id, payload, style_fp, ranking, middle_item_count, execution_date))

        # 비동기 작업 실행
        results = await asyncio.gather(*tasks)

        # 유효한 스타일 정보만 수집
        for result in results:
            if result:
                style_list.append(result)

        return style_list

    async def _process_style(self, style_id, payload, style_fp, ranking, middle_item_count, execution_date):
        try:
            response_data = await self._post(url=self.url, payload=payload, execution_date=execution_date)
            style = self.preprocessor.get_style(response_data[0], style_fp)
            style['rank_score'] = self.preprocessor.get_rank_score(int(ranking), int(middle_item_count))
            return style
        except Exception as e:
            logger.info(f"수집 실패: {style_id} - {e}")
            return None
                
    
