import asyncio
import logging
import random
import requests

import httpx
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from pendulum.datetime import DateTime

logger = logging.getLogger(__name__)

class FetchProductListFromCategoryOperator(BaseOperator): 
    url = 'https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=20&pageSize={ITEMS_COUNT}&pageNo=1&norOutletGbCd=J&dispCtgNo={small_category_num}&productListLayout=4&theditedYn=N'
    
    max_item_counts: int = 100
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
        items = self._gather()
        pids = []
        item_image_urls = {}
        
        print("items : ", items)
        
        for item in items:
            print("item : ", item)
            pids.append(item.get("product_id"))
            product_id = item["product_id"]
            url = item.get("url")
            item_image_urls[product_id] = url
        
        print("pids: ",pids)
        print("item_image_urls: ", item_image_urls)
            
        context["task_instance"].xcom_push(key="item_ids", value=pids)
        context["task_instance"].xcom_push(key="items", value=items)
        context["task_instance"].xcom_push(key="item_image_urls", value=item_image_urls)
    
    def _fetch(self, url: str):
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    
    def _gather(self):
        tasks = []
        category: str 
        
        for category in self.categories_list:
            url = self.url.format(ITEMS_COUNT=self.max_item_counts, small_category_num=category)
            tasks += self._processing(self._fetch(url), category)
            
        return tasks
            
            
    def _processing(self, tasks, category):
        items = []
        
        goods_in_page= tasks['payload']['goodsList']
        
        for rank, goods in enumerate(goods_in_page, 1):
            goods_data = get_item_info(goods)
            goods_data['rank_score'] = get_rank_score(int(rank), len(goods_in_page))
            goods_data['small'] = category
            items.append(goods_data)
        
        return items
         
         
def get_item_info(goods):
    # 상품 id
    goodsNo = goods['goodsNo']

    # 브랜드 명
    brandNm = goods['brandNm']

    # 정가
    norPrc = goods['norPrc']

    # 할인가
    salePrc = goods['salePrc']

    # 이미지    
    image_urls = []      
    try:    
        for i in range(len(goods['colorInfo'])):
            for j in range(len(goods['colorInfo'][i]['colorContInfo'])):
                colorContInfo = goods['colorInfo'][i]['colorContInfo'][j]['dispGoodsContUrl']
                image_urls.append(colorContInfo)
    except:
        pass

    # 색상 정보
    colors = []
    try:
        for color in goods['colorInfo']:
            colors.append(color['optnNm'])
    except:
        pass

    # 사이즈 정보
    sizes = []
    for i in range(len(goods['colorInfo'][0]['colorSizeInfo'])):
        size_info = goods['colorInfo'][0]['colorSizeInfo'][i]['erpSzCd']
        sizes.append(size_info)


    data = {
        'product_id': goodsNo,
        'brand': brandNm,
        'fixed_price': norPrc,
        'discounted_price': salePrc,
        'url': image_urls,
        'colors': colors,
        'sizes': sizes,
    }

    return data

def get_rank_score(ranking, item_count):
    try:
        rank_score = 1 - ((ranking - 1) / (item_count - 1))
    except:
        rank_score = 1

    return rank_score