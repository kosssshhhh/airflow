from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import task, dag, task_group
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

import logging
import pendulum
import json
import httpx

from core.infra.cdn.image_uploader import ImageUploader
from core.infra.cache.decorator import MongoResponseCache
from musinsa.ops.musinsa_preprocess import MusinsaPreprocess, MusinsaReviewPreprocess2
from musinsa_brand.ops.load.style import LoadMusinsaBrandStyle
from musinsa_brand.ops.load.review import LoadMusinsaBrandReview
from musinsa_brand.ops.load.image import LoadMusinsaBrandImage

logger = logging.getLogger(__name__)

# from core.slack.slack_notification import SlackAlert

# slack_api_token = Variable.get('slack_api_token')
# alert = SlackAlert('#airflow-알리미', slack_api_token)

local_tz = pendulum.timezone("Asia/Seoul")

__DEFAULT_ARGS__ = {
    'owner': '400CC',
    'retries': 30,
    'retry_delay': timedelta(minutes=5)
}
__TAGS__ = ["musinsa_brand"]

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

MAX_COUNT = int(Variable.get("musinsa_review_max_count"))
username = Variable.get("username")
password = Variable.get("password")
proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
preprocessor = MusinsaPreprocess()
review_preprocessor = MusinsaReviewPreprocess2()
timeout: float = 120.0

client = httpx.Client(proxies=proxy, headers=headers)

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]
        
@MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.brand.style_list', collection='musinsa.style_ids', date_key='execution_date')
def _get_style_list(url, execution_date=None, key=None):
    response = client.get(url, timeout=timeout)
    return response.json()         

@MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.style.like', collection='musinsa.style_info', payload=True, date_key='execution_date')
def _post_style_info(url, payload, execution_date=None, key=None):
    response = client.post(url, json=payload, timeout=timeout)
    return response.json()

@MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.style.stat', collection='musinsa.style_info', date_key='execution_date')
def _get_style_info_json(url, execution_date=None, key=None):
    response = client.get(url, timeout=timeout)
    return response.json()

@MongoResponseCache(db='musinsa_brand', type='html', key='musinsa.style.info', collection='musinsa.style_info', date_key='execution_date')
def _get_style_info_html(url, execution_date=None, key=None):
    response = client.get(url, timeout=timeout)
    return response.text

@MongoResponseCache(db='musinsa_brand', type='html', key='musinsa.image', collection='musinsa.image', date_key='execution_date')
def _get_image(url, execution_date=None, key=None):
    response = client.get(url, timeout=timeout)
    return response.text

@MongoResponseCache(db='musinsa_brand', type='json', key='musinsa.page.reviews', collection='musinsa.review', date_key='execution_date')
def _fetch_review(url: str, execution_date=None, key=None):
    return _get_review(url).json()
    
def _get_review(url, **kwargs):
    response = client.get(url, timeout=timeout)
    return response

@dag(
    dag_id="musinsa.brand.items",
    start_date= datetime(2024, 6, 18, tzinfo = local_tz),
    schedule="5 9 * * *",
    default_args=__DEFAULT_ARGS__,
    catchup=False,
    tags=__TAGS__,
)
def musinsa_brand_dag():
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    @task(task_id="fetch_styles")
    def fetch_styles_from_brand(brand_group):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_id_list = []
        for brand in brand_group:
            page_num = 1
            while True:
                url =  f'https://api.musinsa.com/api2/dp/v1/plp/goods?brand={brand}&gf=A&sortCode=POPULAR&page={page_num}&size=100&caller=BRAND' 
                response = _get_style_list(url=url, execution_date=execution_date)
                items = response['data']['list']
                if len(items) == 0:
                    break
                
                partial_style_list = [item['goodsNo'] for item in items]
                style_id_list += partial_style_list
                
                page_num += 1
            
            logger.info(f"brand: {brand} is done")

        return style_id_list    
    
    @task(task_id="fetch_styles_info")
    def fetch_style_info(style_id_list):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        style_info_list = []
        
        like_URL = f'https://like.musinsa.com/like/api/v2/liketypes/goods/counts'
        
        for style_id in style_id_list:
            info_URL = f'https://www.musinsa.com/products/{style_id}'
            stat_URL = f'https://goods-detail.musinsa.com/goods/{style_id}/stat'
            
            soup = BeautifulSoup(_get_style_info_html(url=info_URL, execution_date=execution_date),'lxml')
            like_res = _post_style_info(url=like_URL, execution_date=execution_date, payload={'relationIds': [style_id]})
            stat_res = _get_style_info_json(url=stat_URL,execution_date=execution_date)
            
            style_json = preprocessor.parse(soup)
            
            if style_json is None:
                continue
            
            style_info = preprocessor.processing_info(style_json)
            style_like = preprocessor.processing_like(like_res)
            style_stat = preprocessor.processing_stat(stat_res)
            
            merged_data = {**style_info, **style_stat, 'like': style_like}
            
            style_info_list.append(merged_data)
            
        return style_info_list
    
    
    @task(task_id="fetch_images")
    def fetch_images(style_id_list):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        styles_image_dict = {}
        for style_id in style_id_list:
            url = f'https://www.musinsa.com/products/{style_id}'
            soup = BeautifulSoup(_get_image(url=url, execution_date=execution_date),'lxml')
            style_json = preprocessor.parse(soup)
            
            if style_json is None:
                continue
            
            style_image = preprocessor.processing_image(style_json)
            
            styles_image_dict[style_id] = style_image
            
        return styles_image_dict
    
    @task(task_id="transform_images")
    def transform_images(images_dict):
        uploader = ImageUploader(bucket_name="designovel", folder_name="musinsa")
        image_url_list = uploader.upload_images(images_dict)
        return image_url_list
    
    @task(task_id="fetch_reviews")
    def fetch_reviews(style_id_list):
        max_review_counts: int = MAX_COUNT
        review_list = []
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        for style_id in style_id_list:
            url = f'https://goods.musinsa.com/api2/review/v1/view/list?page=0&pageSize={max_review_counts}&goodsNo={style_id}&sort=up_cnt_desc&myFilter=false&selectedSimilarNo={style_id}'
            review_list += review_preprocessor.get_review(_fetch_review(url=url, execution_date=execution_date), style_id)
            
        return review_list
    
    @task_group(group_id="fetch_tasks")
    def process_brand(brand_group):
        style_id_list = fetch_styles_from_brand(brand_group)
        style_info = fetch_style_info(style_id_list)
        images = fetch_images(style_id_list)
        image_url_list = transform_images(images)
        review = fetch_reviews(style_id_list) 
        
        return style_info, image_url_list, review
    
    @task
    def load_styles(style_info):
        style_infos = []
        for info in style_info:
            style_infos.extend(info)
        logger.info(f"style_infos : {style_infos}")
        load_operator = LoadMusinsaBrandStyle(task_id="load_styles")
        context = get_current_context()
        load_operator.execute(context=context, style_info=style_infos)
        
    @task
    def load_images(cdn_image_url):
        cdn_image_urls = {}
        for url in cdn_image_url:
            cdn_image_urls.update(url)
        logger.info(f'cdn_image_urls : {cdn_image_urls}')
        load_operator = LoadMusinsaBrandImage(task_id="load_images")
        context = get_current_context()
        load_operator.execute(context=context, cdn_image_url=cdn_image_urls)
        
    @task
    def load_reviews(style_review_list, execution_date):
        style_review_lists = []        
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        for review in style_review_list:
            style_review_lists.extend(review)
        load_operator = LoadMusinsaBrandReview(task_id="load_reviews")
        load_operator.execute(context=context, style_review_list=style_review_lists, execution_date=execution_date)

    process_results = process_brand.expand(brand_group=list(chunk_list(json.loads(Variable.get("musinsa_brands")), 3)))        
    start >> process_results
    
    load_styles(style_info=process_results[0]) >> end
    load_images(cdn_image_url=process_results[1]) >> end
    load_reviews(style_review_list=process_results[2]) >> end
    
    # load_features(cdn_image_url=process_results[2], style_info=process_results[1])

# DAG 인스턴스 생성
musinsa_brand_dag_instance = musinsa_brand_dag()
