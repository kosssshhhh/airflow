import logging
import requests
import numpy as np

# import httpx
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.context import Context
from pendulum.datetime import DateTime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
MAX_COUNT = 100

# TODO: DB에서 불러오기
middle_category_list = ['10101201', '10101202', '10101203', '10101204', '10101205',
                        '10101206', '10101207', '10101208', '10101209', '10101210',
                        '10101211', '10101212']

class FetchProductListFromCategoryOperator(BaseOperator): 
    url = 'https://api-display.wconcept.co.kr/display/api/v2/best/products'
    max_item_counts: int = MAX_COUNT
    middle_category_list = middle_category_list
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
}
    
    def execute(
        self,
        context: Context,
    ):
        # items = asyncio.
        total_product_list, total_product_info_list = self._gather()

        context["task_instance"].xcom_push(key="product_id_list", value=total_product_list)
        context["task_instance"].xcom_push(key="product_list", value=total_product_info_list)

    
    def _fetch(self, url, payload):
        return self._post(url, payload).json()
        
    
    def _post(self, url, payload):
        response = requests.post(url, headers=self.headers, json=payload)
        return response
 
    
    def _gather(self):
        total_product_list = []
        total_product_info_list = []
        
        for gender in ['men', 'women']:
            for middle_category in self.middle_category_list:
                url = self.url
                
                if gender == 'men':
                    genderType = 'men'
                else:
                    genderType = 'women'
                    
                payload = {
                    "custNo": "",
                    "dateType": "daily",
                    "domain": 'WOMEN',
                    "genderType": genderType,
                    "depth1Code": "10101",
                    "depth2Code": middle_category,
                    "pageNo": 1,
                    "pageSize": self.max_item_counts
                    }                
                
                product_list, product_info_list = self._processing(self._fetch(url, payload))
                
                total_product_list += product_list
                total_product_info_list += product_info_list
                
        return total_product_list, total_product_info_list
                
        
    def _processing(self, tasks):
        product_list = []
        product_info_list = []
        
        products_in_page= tasks['data']['content']
        
        for product in products_in_page:
            product_list.append(product['itemCd'])
            product_info_list.append([product['itemCd'], product['finalPrice']])
        
        product_info_list = np.array(product_info_list)
        rankings = np.arange(1, len(product_info_list) + 1)
        total_items_count = np.full(len(product_info_list), len(product_info_list))
        
        product_info_list= np.column_stack((product_info_list, rankings, total_items_count))
        product_info_list = product_info_list.tolist()
        
        return product_list, product_info_list
        

class FetchProductOperator(BaseOperator):
    url = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
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
        

    
    def _fetch(self, url, payload):
        return self._post(url, payload).json()[0]
    
    def _post(self, url, payload):
        response = requests.post(url, headers=self.headers, data=payload)
        return response
 
    
    def _gather(self, xcomData):
        product_list = []
        
        for product_id, product_fp, ranking, middle_item_count in xcomData: 
            payload = {"itemcds": int(product_id)}

            product = self._processing(self._fetch(self.url, payload), product_fp)
            product['rank_score'] = get_rank_score(int(ranking), int(middle_item_count))
            product_list.append(product)
            
        return product_list 
                
        
    def _processing(self, tasks, product_fp):
        # 브랜드
        brandNameKr = tasks['brandNameKr']

        # 품번
        itemCd = tasks['itemCd']

        # 좋아요
        heartCnt = tasks['heartCnt']

        # 품절 여부
        statusName = tasks['statusName']

        # 정상가
        fixed_price = tasks['customerPrice']

        # 쿠폰 적용가 
        discounted_price = product_fp

        # 색상
        color = tasks['color1']
        
        # 카테고리
        categories = tasks['category']
        category_per_depth = []
        for category in categories:
            medium_name = category['mediumName']
            category_depthname1 = category['categoryDepthname1']
            category_depthname2 = category['categoryDepthname2']
            category_depthname3 = category['categoryDepthname3']
            
            dic = {'medium_name': medium_name,
                'category_depthname1': category_depthname1,
                'category_depthname2': category_depthname2,
                'category_depthname3': category_depthname3}
            
            category_per_depth.append(dic)


        # 상품명
        itemName = tasks['itemName']
        
        product = {
        'brand': brandNameKr,
        'product_id': itemCd,
        'likes': heartCnt,
        'sold_out': statusName,
        'fixed_price': fixed_price,
        'discounted_price': discounted_price,
        'product_name': itemName,
        'category_per_depth': category_per_depth,
        'color': color,
        }   
        
        return product
        
    
    
    
    
def get_rank_score(ranking, total_items_count):
    return 1 - (ranking / total_items_count)