import logging
import requests
import numpy as np
import re
from bs4 import BeautifulSoup

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context



logger = logging.getLogger(__name__)


class FetchReviewOperator(BaseOperator):
# TODO: FetchProductImageOperator
    payload_URL = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
    review_URL = "https://www.wconcept.co.kr/Ajax/ProductReViewList"
    
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
        
        products_reviews_list = self._gather(xcomData)
        logger.info(f"result : {products_reviews_list}")
        context["task_instance"].xcom_push(key="product_review", value=products_reviews_list)
        

    
    def _fetch(self, url, payload):
        return self._post(url, payload)
    
    def _post(self, url, payload):
        response = requests.post(url, headers=self.headers, data=payload)
        return response
 
    
    def _gather(self, xcomData):
        products_reviews_list = []
        payloads_list = []
        for product_id in xcomData: 
            payload = {"itemcds": product_id}
            
            fetch_data = self._fetch(self.payload_URL, payload).json()[0]
            
            item_payloads = [fetch_data['itemCd'], fetch_data['category'][0]['mediumCd'], fetch_data['category'][0]['categoryCd'], fetch_data['itemTypeCd']]
            
            payloads_list.append(item_payloads)
        
        for item_payload in payloads_list:
            product_reviews_list =[]
            i = 1
            
            while True:
                payload = {
                    'itemcd': item_payload[0],
                    'pageIndex': i,
                    'order': 1,
                    'IsPrdCombinOpt': 'N',
                    'mediumcd': item_payload[1],
                    'categorycd': item_payload[2],
                    'itemtypecd': item_payload[3]
                }
                
                soup = BeautifulSoup(self._fetch(self.review_URL, payload).text, 'lxml')
                
                review_count = len(soup.select('p.pdt_review_text'))
                
                if review_count == 0:
                    break
                
                for j in range(review_count):
                    review = self._parse(j, soup, item_payload[0])
                    product_reviews_list.append(review)
                
                i += 1
            
            if len(product_reviews_list) == 0:
                continue
            else:
                products_reviews_list += product_reviews_list
                
        return products_reviews_list
                

                
    def _parse(self, index, soup, product_id):
        # 구매 옵션과 사이즈정보 빼내기 용
        review_info = soup.select('div.pdt_review_info')[index]

        # 리뷰 id
        review_id = soup.select('div.product_review_reaction > div > button.link_txt.open-layer.open-pop_review_report')[index]['data-idxnum']
        
        # 구매 옵션
        try:
            option = review_info.select('div.pdt_review_option > p')[0].text.strip()
            option = option.split(':')[1].strip()
        except:
            option = None

        # 사이즈 정보
        try:
            cust_size_info = review_info.select('div.pdt_review_option > p')[1].text.strip()
            cust_size_info = cust_size_info.split(':')[1].strip()
        except:
            cust_size_info = None

        # 사이즈, 색상, 소재 빼내기 용
        try:
            sku = soup.select('ul.product_review_evaluation')[index]       

            # 사이즈
            size = sku.select('ul.product_review_evaluation > li > div > em')[0].text

            # 색상
            color = sku.select('ul.product_review_evaluation > li > div > em')[1].text

            # 소재
            texture = sku.select('ul.product_review_evaluation > li > div > em')[2].text
            
        except:
            size, color, texture = None, None, None
    
            # user id
        user_id = soup.select('p.product_review_info_right > em')[index].text

        # 작성 시간
        time = soup.select('p.product_review_info_right > span')[index].text

        # 리뷰 내용
        content = soup.select('p.pdt_review_text')[index].text.strip()

        # rating 정보
        rating_pct = soup.select('div.star-grade > strong[style]')[index]
        rating = re.findall(r'\d+', str(rating_pct))[0]
        rating = int(int(rating) / 20)

        # 좋아요 개수
        favorite = soup.select('button.like.btn_review_recommend')[index].text
        
        data = {
            'product_id': product_id,
            'review_id': review_id,
            'purchase_option': option,
            'size_info': cust_size_info,
            'size': size,
            'color': color,
            'material': texture,
            'user_id': user_id,
            'written_time': time,
            'body': content,
            'rate': rating,
            'likes': favorite
        }   

        return data
    
        
    
    
    
    
def get_rank_score(ranking, total_items_count):
    return 1 - (ranking / total_items_count)