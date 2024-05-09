import logging
import requests
from bs4 import BeautifulSoup
import time

# import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

class FetchReviewOperator(BaseOperator):
    URL = 'https://goods.musinsa.com/api/goods/v2/review/{review_type}/list?similarNo=0&sort=up_cnt_desc&selectedSimilarNo={product_id}&page={page_num}&goodsNo={product_id}&rvck=202404150551&_=1713416512533'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
    
    def execute(
        self,
        context: Context,
    ):
        task_instance: TaskInstance = context["task_instance"]
        xcomData = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        logger.info(f"xcomData : {xcomData}")
        
        product_review_result = self._gather(xcomData)
        
        context["task_instance"].xcom_push(key="product_review", value=product_review_result)
        logger.info(f"product_review_count : {len(product_review_result)}")

    
    def _fetch(self, url):
        return self._get(url)
    
    
    # TODO: decorator 추가
    def _get(self, url):
        response = requests.get(url, headers=self.headers)
        return response
    
    def _gather(self, xcomData):
        product_review_list = []
        review_types = ['style', 'goods', 'photo']
        
        
        for product_id in xcomData:
            for review_type in review_types:
                page_num = 1    
                while True:
                    url = self.URL.format(review_type=review_type, product_id=product_id, page_num=page_num)
                    soup = BeautifulSoup(self._fetch(url).text, 'lxml')
                    
                    if not soup.select('p.review-profile__name'):
                        break
                    
                    one_page_review = self._parse(soup, product_id, review_type)
                    logger.info(f"one_page_review : {one_page_review}")
                    
                    product_review_list += one_page_review
                    
                    page_num += 1
                logger.info(f"product_review_list : {product_review_list}")
            time.sleep(0.5)
        
        logger.info(f"product_review_count : {len(product_review_list)}")
        return product_review_list 
    
    
    def _parse(self, soup, product_id, review_type):
        one_page_reviews = []
        reviews = soup.select('div.review-list')
        
        for review in reviews:
            review_dict = self._processing(review, product_id)
            review_dict['review_type'] = review_type
            logger.info(f"review_dict : {review_dict}")
            one_page_reviews.append(review_dict)
            
        return one_page_reviews
    
    
    def _processing(self, review, product_id):
        # 댓글 단 사람
        user_name = review.select('p.review-profile__name')[0].text

        # 리뷰 id
        review_id = review.select('div.review-contents')[0]['data-review-no']
        
        # 메타 데이터
        meta_dict = {}
        metas = review.select('li.review-evaluation--type3__item')
        for meta in metas:
            key, value = meta.text.split(' ', 1)  # 공백을 기준으로 처음 나오는 부분만 분리
            meta_dict[key] = value

        # 리뷰 내용
        content = review.select('div.review-contents__text')[0].text

        # 배지
        badge = review.select('span.review-evaluation-button--type3__count')

        # 도움돼요
        helpful = badge[0].text

        # 스타일 좋아요
        try:
            style_good = badge[1].text
            
        except:
            style_good = None

        # 별 개수
        star_percentage = review.select('span.review-list__rating__active')[0]['style']

        if star_percentage == 'width: 100%':
            star = 5
        elif star_percentage == 'width: 80%':
            star = 4
        elif star_percentage == 'width: 60%':
            star = 3
        elif star_percentage == 'width: 40%':
            star = 2
        elif star_percentage == 'width: 20%':
            star = 1
        else:
            star = 0

        data = {
            'product_id': product_id,
            'review_id': review_id,
            'user_info': user_name,
            'meta_data': meta_dict,
            'body': content,
            'helpful': helpful,
            'good_style': style_good,
            'rate': star
        }

        return data
    