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

logger = logging.getLogger(__name__)
MAX_COUNT = 1

# TODO: DB에서 불러오기
middle_category_list = ['001006', '001004', '001005', '001010', '001002', '001003',
                        '001001', '001011', '001013', '001008', '002022', '002001',
                        '002002', '002025', '002017', '002003', '002020', '002019',
                        '002023', '002018', '002004', '002008', '002007', '002024',
                        '002009', '002013', '002012', '002016', '002021', '002014',
                        '002006', '002015', '003002', '003007', '003008', '003004',
                        '003009', '003005', '003010', '003011', '003006', '002006',
                        '002007', '002008', '022001', '022002', '022003']

class FetchProductListFromCategoryOperator(BaseOperator): 
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

        context["task_instance"].xcom_push(key="product_id_list", value=total_product_id_list)
        context["task_instance"].xcom_push(key="product_list", value=total_product_list)

    
    def _fetch(self, url):
        return self._get(url)
        
    
    def _get(self, url):
        response = requests.get(url, headers=self.headers)
        return response
 
    
    def _gather(self):
        total_product_list = [] # 모든 카테고리 4700개 전체 상품 
        
        for middle_category in self.middle_category_list:
            page = 1
            url = self.url.format(category_number=middle_category, page=page)
            soup = BeautifulSoup(self._fetch(url).text, 'lxml')
            
            flag = 0
            
            total_page = int(soup.select('span.totalPagingNum')[0].text)
            
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
                soup = BeautifulSoup(self._fetch(url).text, 'lxml')
                

            pd_list = np.array(pd_list)
            
            rankings = np.arange(1, len(pd_list) + 1)
            total_items_count = np.full(len(pd_list), len(pd_list))
            rank_score = get_rank_score(rankings, total_items_count)
            
            pd_list = np.column_stack((pd_list, rank_score)).tolist()
            
            total_product_list += pd_list    
            
            logger.info(f"middle_category: {middle_category} is done")
                
        return total_product_list
                

def get_rank_score(ranking, total_items_count):
    return 1 - (ranking / total_items_count)
            
        
        
        

        

class FetchProductOperator(BaseOperator):
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
        

    
    def _fetch_get(self, url, ):
        return self._get(url, )
    
    def _fetch_post(self, url, payload):
        return self._post(url, payload)
    
    # TODO: decorator 추가
    def _post(self, url, payload):
        response = requests.post(url, headers=self.headers, json=payload)
        return response
 
    def _get(self, url):
        response = requests.get(url, headers=self.headers)
        return response
    
    def _gather(self, xcomData):
        product_list = []
        
        for product_id, middle_category, rank_score in xcomData: 
            
            soup = BeautifulSoup(self._fetch_get(self.info_URL.format(product_id=product_id)).text, 'lxml')
            like_res = self._fetch_post(self.like_URL, {'relationIds': [product_id]})
            stat_res = self._fetch_get(self.stat_URL.format(product_id=product_id))
            
            
            product_json = self._parse(soup)
            
            if product_json is None:
                continue
            
            product_info = self._processing(product_json)
            
            product_like = self._processing_like(like_res)
            
            product_stat = self._processing_stat(stat_res)
            
            merged_data = {**product_info, **product_stat, 'like': product_like, 'rank_score': rank_score, 'middle_category': middle_category}
            
            product_list.append(merged_data)
            
            logger.info(f"product_id: {product_id} is done")
            
            
        return product_list 
    
    def _processing_stat(self, tasks):
        try:
            cumalative_sales = tasks.json()['data']['purchase']['total']
        except:
            cumalative_sales = None
            
        try:
            ages = tasks.json()['data']['purchase']['rates']
            ages = {key: f"{value}%" for key, value in ages.items()}
            under_18 = f"{ages['AGE_UNDER_18']}%"
            age_19_to_23 = f"{ages['AGE_19_TO_23']}%"
            age_24_to_28 = f"{ages['AGE_24_TO_28']}%"
            age_29_to_33 = f"{ages['AGE_29_TO_33']}%"
            age_34_to_39 = f"{ages['AGE_34_TO_39']}%"
            over_40 = f"{ages['AGE_OVER_40']}%"
        except:
            under_18, age_19_to_23, age_24_to_28, age_29_to_33, age_34_to_39, over_40 = None, None, None, None, None, None

        # 성비
        try:
            male = int(tasks.json()['data']['purchase']['male'])
            female = int(tasks.json()['data']['purchase']['female'])
            total_count = male + female
            male_percentage = int(round((male / total_count) * 100, -1))
            female_percentage = int(round((female / total_count) * 100, -1))
            male_percentage = male_percentage
            female_percentage = female_percentage
        except:
            male_percentage = None
            female_percentage = None
            
        return {
            'cumalative_sales': cumalative_sales,
            'under_18': under_18,
            'age_19_to_23': age_19_to_23,
            'age_24_to_28': age_24_to_28,
            'age_29_to_33': age_29_to_33,
            'age_34_to_39': age_34_to_39,
            'over_40': over_40,
            'male_percentage': male_percentage,
            'female_percentage': female_percentage
        }
                
    def _processing_like(self, tasks):
        return tasks.json()['data']['contents']['items'][0]['count']
        
        
    def _processing(self, tasks):   
        data = {
            'product_id': tasks['goodsNo'],
            'brand': tasks['brand'],
            'product_num': tasks['styleNo'],
            'fixed_price': tasks['goodsPrice']['originPrice'],
            'discounted_price': tasks['goodsPrice']['minPrice'],
        }

        return data
        
    
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
        
    
    
    
    
def get_rank_score(ranking, total_items_count):
    return 1 - (ranking / total_items_count)