import asyncio
import random
from bs4 import BeautifulSoup
import pandas as pd
import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.context import Context

# Constants
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    # Add more user agents as needed
]

class FetchProductLinksOperator(BaseOperator):
    """
    Fetch product links from Musinsa based on given category numbers.
    """
    template_fields = ("category_nums",)
    
    def __init__(self, category_nums, limit=100, **kwargs):
        super().__init__(**kwargs)
        self.category_nums = category_nums
        self.limit = limit
        
    def execute(self, context: Context):
        links = asyncio.run(self._gather_product_links())
        context["task_instance"].xcom_push(key="product_links", value=links)
        return links

    async def _fetch_links(self, category_num):
        url = f"https://www.musinsa.com/categories/item/{category_num}"
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        products = soup.select('a.img-block')
        return [product['href'] for product in products[:self.limit]]

    async def _gather_product_links(self):
        tasks = [self._fetch_links(num) for num in self.category_nums]
        results = await asyncio.gather(*tasks)
        product_links = [link for sublist in results for link in sublist]
        return product_links[:self.limit]

class FetchProductDetailsOperator(BaseOperator):
    """
    Fetch detailed product information and parse it.
    """
    template_fields = ("product_urls",)

    def __init__(self, product_urls, **kwargs):
        super().__init__(**kwargs)
        self.product_urls = product_urls

    def execute(self, context: Context):
        product_details = asyncio.run(self._gather_product_details())
        context["task_instance"].xcom_push(key="product_details", value=product_details)
        return product_details

    async def _fetch_details(self, url):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        # Add your parsing logic here
        return {"url": url, "title": soup.title.string}  # Example structure

    async def _gather_product_details(self):
        tasks = [self._fetch_details(url) for url in self.product_urls]
        results = await asyncio.gather(*tasks)
        return results
