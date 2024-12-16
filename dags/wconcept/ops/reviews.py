import logging
import requests
import numpy as np
import re
from bs4 import BeautifulSoup
import httpx
import asyncio

from airflow.models.variable import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from wconcept.ops.wconcept_preprocess import WconceptReviewPreprocess
from core.infra.cache.decorator import MongoResponseCache, MongoAsyncResponseCache


logger = logging.getLogger(__name__)


class FetchReviewOperator(BaseOperator):
    preprocessor = WconceptReviewPreprocess()

    payload_URL = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    review_URL = "https://www.wconcept.co.kr/Ajax/ProductReViewList"
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
        
        styles_reviews_list = self._gather(xcomData, execution_date)
        logger.info(f"수집된 리뷰 수 : {len(styles_reviews_list)} ")
        context["task_instance"].xcom_push(key="style_review", value=styles_reviews_list)


    @MongoResponseCache(db='wconcept', type='json', key='wconcept.review.payload', collection='wconcept.review', payload=True, date_key='execution_date')
    def _post_json(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, data=payload, timeout=self.timeout)

        return response.json()


    @MongoResponseCache(db='wconcept', type='html', key='wconcept.review', collection='wconcept.review', payload=True, date_key='execution_date')
    def _post_html(self, url, payload, execution_date=None, key=None):
        response = self.client.post(url, data=payload, timeout=self.timeout)

        return response.text
    
 
    
    def _gather(self, xcomData, execution_date):
        styles_reviews_list = []
        payloads_list = []
        for style_id in xcomData: 
            payload = {"itemcds": style_id}
            
            try:
                fetch_data = self._post_json(url=self.payload_URL, payload=payload, execution_date=execution_date)[0]
            except:
                logger.info(f"수집 실패: {style_id} ")
                continue
            
            request_payload = [fetch_data['itemCd'], fetch_data['category'][0]['mediumCd'], fetch_data['category'][0]['categoryCd'], fetch_data['itemTypeCd']]
            
            payloads_list.append(request_payload)
        
        for request_payload in payloads_list:
            style_reviews_list =[]
            
            
            for page in range(1,6):
                payload = {
                    'itemcd': request_payload[0],
                    'pageIndex': page,
                    'order': 1,
                    'IsPrdCombinOpt': 'N',
                    'mediumcd': request_payload[1],
                    'categorycd': request_payload[2],
                    'itemtypecd': request_payload[3]
                }
                
                reviews_in_page = BeautifulSoup(self._post_html(url=self.review_URL, payload=payload, execution_date=execution_date), 'lxml')
                
                page_review_counts = len(reviews_in_page.select('p.pdt_review_text'))
                
                if page_review_counts == 0:
                    break
                
                for page_count in range(page_review_counts):
                    review = self.preprocessor.get_review_data(page_count, reviews_in_page, request_payload[0])
                    style_reviews_list.append(review)
                
            
            if len(style_reviews_list) == 0:
                continue
            else:
                styles_reviews_list += style_reviews_list
                
        return styles_reviews_list
                


class FetchReviewAsyncOperator(BaseOperator):
    preprocessor = WconceptReviewPreprocess()

    payload_URL = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    review_URL = "https://www.wconcept.co.kr/Ajax/ProductReViewList"
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

        styles_reviews_list = await self._gather(xcomData, execution_date)
        logger.info(f"수집된 리뷰 수 : {len(styles_reviews_list)}")
        context["task_instance"].xcom_push(key="style_review", value=styles_reviews_list)

    @MongoAsyncResponseCache(db='wconcept', type='json', key='wconcept.review.payload', collection='wconcept.review', payload=True, date_key='execution_date')
    async def _post_json(self, url, payload, execution_date=None, key=None):
        response = await self.async_client.post(url, data=payload, timeout=self.timeout)
        return response.json()

    @MongoAsyncResponseCache(db='wconcept', type='html', key='wconcept.review', collection='wconcept.review', payload=True, date_key='execution_date')
    async def _post_html(self, url, payload, execution_date=None, key=None):
        response = await self.async_client.post(url, data=payload, timeout=self.timeout)
        return response.text
    
    async def _gather(self, xcomData, execution_date):
        styles_reviews_list = []
        payloads_list = []

        # 여러 스타일 ID에 대해 비동기 요청 생성
        tasks = []
        for style_id in xcomData:
            payload = {"itemcds": style_id}
            tasks.append(self._post_json(url=self.payload_URL, payload=payload, execution_date=execution_date))

        responses = await asyncio.gather(*tasks)

        # 각 스타일에 대해 데이터 처리
        for response in responses:
            try:
                fetch_data = response[0]  # response는 JSON 포맷
                request_payload = [fetch_data['itemCd'], fetch_data['category'][0]['mediumCd'], fetch_data['category'][0]['categoryCd'], fetch_data['itemTypeCd']]
                payloads_list.append(request_payload)
            except:
                logger.info(f"수집 실패: {style_id}")
                continue

        # 리뷰 데이터를 비동기적으로 처리
        tasks = []
        for request_payload in payloads_list:
            tasks.append(self._fetch_reviews_for_payload(request_payload, execution_date))

        review_results = await asyncio.gather(*tasks)

        # 수집한 리뷰를 리스트에 추가
        for reviews in review_results:
            styles_reviews_list.extend(reviews)

        return styles_reviews_list

    async def _fetch_reviews_for_payload(self, request_payload, execution_date):
        style_reviews_list = []
        for page in range(1, 6):
            payload = {
                'itemcd': request_payload[0],
                'pageIndex': page,
                'order': 1,
                'IsPrdCombinOpt': 'N',
                'mediumcd': request_payload[1],
                'categorycd': request_payload[2],
                'itemtypecd': request_payload[3]
            }

            reviews_in_page_html = await self._post_html(url=self.review_URL, payload=payload, execution_date=execution_date)
            reviews_in_page = BeautifulSoup(reviews_in_page_html, 'lxml')
            page_review_counts = len(reviews_in_page.select('p.pdt_review_text'))

            if page_review_counts == 0:
                break

            for page_count in range(page_review_counts):
                review = self.preprocessor.get_review_data(page_count, reviews_in_page, request_payload[0])
                style_reviews_list.append(review)

        return style_reviews_list