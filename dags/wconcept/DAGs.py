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

from core.infra.cache.decorator import MongoResponseCache
from core.infra.cdn.image_uploader import ImageUploader
from wconcept.ops.wconcept_preprocess import WconceptPreprocess, WconceptReviewPreprocess
from wconcept.ops.load.style import LoadWConceptStyle
from wconcept.ops.load.feature import LoadWConceptFeature
from wconcept.ops.load.review import LoadWConceptReview
from wconcept.ops.load.image import LoadWConceptImage

# from core.slack.slack_notification import SlackAlert

# slack_api_token = Variable.get('slack_api_token')
# alert = SlackAlert('#airflow-알리미', slack_api_token)

local_tz = pendulum.timezone("Asia/Seoul")
MAX_COUNT = int(Variable.get("wconcept_max_count"))
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': '400CC',
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
}

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Display-Api-Key': 'VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk='
        }
username = Variable.get("username")
password = Variable.get("password")
proxy = f"socks5://{username}:{password}@gate.smartproxy.com:7000"
preprocessor = WconceptPreprocess()
timeout: float = 120.0

client = httpx.Client(proxies=proxy, headers=headers)

@MongoResponseCache(db='wconcept', type='json', key='wconcept.style_list', collection='wconcept.topK_styles', payload=True, date_key='execution_date')
def _post_style_list(url, payload, execution_date=None, key=None):
    response = client.post(url, json=payload, timeout=timeout)
    return response.json()

@MongoResponseCache(db='wconcept', type='json', key='wconcept.style', collection='wconcept.style_info', payload=True, date_key='execution_date')
def _post_style_info(url, payload, execution_date=None, key=None):
    response = client.post(url, data=payload, timeout=timeout)
    return response.json()

@MongoResponseCache(db='wconcept', type='json', key='wconcept.review.payload', collection='wconcept.review', payload=True, date_key='execution_date')
def _post_review_json(url, payload, execution_date=None, key=None):
    response = client.post(url, data=payload, timeout=timeout)

    return response.json()

@MongoResponseCache(db='wconcept', type='html', key='wconcept.review', collection='wconcept.review', payload=True, date_key='execution_date')
def _post_review_html(url, payload, execution_date=None, key=None):
    response = client.post(url, data=payload, timeout=timeout)

    return response.text

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

@dag(
    dag_id="wconcept.items",
    start_date=datetime(2024, 6, 18, tzinfo=local_tz),
    schedule_interval="5 9 * * *",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["wconcept"]
)
def wconcept_items_dag():
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    @task(task_id="fetch_styles")
    def fetch_styles_from_category(category_group):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        style_id_list = []
        style_list = []
        logger.info(f'category_group:{category_group}')
        
        for category in category_group:
            url = 'https://api-display.wconcept.co.kr/display/api/v2/best/products'
            logger.info(f'category:{category}')
            for gender in ['men', 'women']:
                payload = preprocessor.get_payload(MAX_COUNT, category, gender)
                response = _post_style_list(url=url, payload=payload, execution_date=execution_date)

                style_ids, style_info_list = preprocessor.get_style_and_style_info_list(response)
                style_id_list += style_ids
                style_list += style_info_list

        return {"style_id_list": style_id_list, "style_list": style_list}


    @task(task_id="fetch_styles_info")
    def fetch_style_info(results):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        style_list = results['style_list']
        preprocessor = WconceptPreprocess()
        url = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
        style_info_list = []

        for style_id, style_fp, ranking, middle_item_count in style_list:
            payload = {"itemcds": int(style_id)}
            try:
                style = preprocessor.get_style(_post_style_info(url=url, payload=payload, execution_date=execution_date)[0], style_fp)
                style['rank_score'] = preprocessor.get_rank_score(int(ranking), int(middle_item_count))
                style_info_list.append(style)
            except Exception as e:
                logger.info(f"수집 실패: {style_id} - {e}")

        logger.info(f"수집된 상품 수: {len(style_info_list)}")
        return style_info_list

    @task(task_id="fetch_images")
    def fetch_images(results):
        
        preprocessor = WconceptPreprocess()
        url = 'https://www.wconcept.co.kr/Product/{style_id}?rccode=pc_topseller'
        styles_image_dict = {}

        style_id_list = results['style_id_list']
        
        for style_id in style_id_list:
            try:
                response = httpx.get(url.format(style_id=style_id))
                soup = BeautifulSoup(response.text, 'lxml')
                imageUrls = preprocessor.parse_image(soup)
                styles_image_dict[style_id] = imageUrls
            except Exception as e:
                logger.info(f"이미지 파싱 실패: {style_id} - {e}")
                styles_image_dict[style_id] = []

        logger.info(f"len(style_image_url): {len(styles_image_dict)}")
        return styles_image_dict

    @task(task_id="transfor_images")
    def transform_images(images_dict):
        uploader = ImageUploader(bucket_name="designovel", folder_name="wconcept")
        image_url_list = uploader.upload_images(images_dict)
        return image_url_list

    @task(task_id="fetch_reviews")
    def fetch_reviews(results):
        context = get_current_context()
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        style_id_list = results['style_id_list']
        preprocessor = WconceptReviewPreprocess()
        payload_URL = "https://www.wconcept.co.kr/Ajax/GetProductsInfo"
        review_URL = "https://www.wconcept.co.kr/Ajax/ProductReViewList"
        review_list = []
        payloads_list = []
        
        for style_id in style_id_list:
            payload = {"itemcds": style_id}
            try:
                fetch_data = _post_review_json(url=payload_URL, payload=payload, execution_date=execution_date)[0]
            except:
                logger.info(f"수집 실패: {style_id} ")
                continue
            
            request_payload = [fetch_data['itemCd'], fetch_data['category'][0]['mediumCd'], fetch_data['category'][0]['categoryCd'], fetch_data['itemTypeCd']]
            
            payloads_list.append(request_payload)
            
        for request_payload in payloads_list:
            style_reviews_list = []

            for page in range(1, 6):
                payload = {
                    "itemcd": request_payload[0],
                    "pageIndex": page, 
                    "order": 1, 
                    "IsPrdCombinOpt": 'N',
                    "mediumcd": request_payload[1],
                    "categorycd": request_payload[2],
                    "itemtypecd": request_payload[3]
                }
                
                try:
                    reviews_in_page = BeautifulSoup(_post_review_html(url=review_URL, payload=payload, execution_date=execution_date), 'lxml')

                    page_review_counts = len(reviews_in_page.select('p.pdt_review_text'))
                    
                    if page_review_counts == 0:
                        break

                    for page_count in range(page_review_counts):
                        review = preprocessor.get_review_data(page_count, reviews_in_page, request_payload[0])
                        style_reviews_list.append(review)

                    logger.info(f"{style_id}의 {page} 페이지에서 리뷰 {page_review_counts}건 수집 완료")

                except Exception as e:
                    logger.error(f"리뷰 수집 실패: {style_id} - {e}")
                    break
                
                logger.info(f"{style_id}의 {page} 페이지에서 리뷰 {page_review_counts}건 수집 완료")

            if len(style_reviews_list) > 0:
                review_list += style_reviews_list

        logger.info(f"전체 수집된 리뷰 수: {len(review_list)}")
        return review_list

    @task_group(group_id="fetch_tasks")
    def process_category(category_group):
        results = fetch_styles_from_category(category_group)
        style_info = fetch_style_info(results)
        images = fetch_images(results)
        image_url_list = transform_images(images)
        review = fetch_reviews(results) 
        
        return results, style_info, image_url_list, review

    @task
    def load_styles(results, style_info):
        style_id_lists = []
        for result in results:
            style_id_lists.extend(result['style_id_list'])
        logger.info(f"style_id_lists : {style_id_lists}")
        style_infos = []
        for info in style_info:
            style_infos.extend(info)
        logger.info(f"style_infos : {style_infos}")
        
        load_operator = LoadWConceptStyle(task_id="load_styles")
        context = get_current_context()
        load_operator.execute(context=context, style_id_list=style_id_lists, style_info=style_infos)

        
    @task
    def load_features(cdn_image_url, style_info):
        cdn_image_urls = []
        for url in cdn_image_url:
            cdn_image_urls.extend(url)
        style_infos = []
        for info in style_info:
            style_infos.extend(info)
        load_operator = LoadWConceptFeature(task_id="load_features")
        context = get_current_context()
        load_operator.execute(context=context, cdn_image_url=cdn_image_urls, style_info=style_infos)

        
    @task
    def load_images(cdn_image_url):
        cdn_image_urls = {}
        for url in cdn_image_url:
            cdn_image_urls.update(url)
        logger.info(f'cdn_image_urls : {cdn_image_urls}')
        load_operator = LoadWConceptImage(task_id="load_images")
        context = get_current_context()
        load_operator.execute(context=context, cdn_image_url=cdn_image_urls)
        
    @task
    def load_reviews(style_review_list, execution_date):
        style_review_lists = []
        for review in style_review_list:
            style_review_lists.extend(review)
        load_operator = LoadWConceptReview(task_id="load_reviews")
        context = get_current_context()
        load_operator.execute(context=context, style_review_list=style_review_lists, execution_date=execution_date)

    process_results = process_category.expand(category_group=list(chunk_list(json.loads(Variable.get(key="wconcept_category_nums")), 2)))        
    start >> process_results
    
    load_styles(results=process_results[0], style_info=process_results[1]) >> end
    load_images(cdn_image_url=process_results[2]) >> end
    load_reviews(style_review_list=process_results[3]) >> end
    
    # load_features(cdn_image_url=process_results[2], style_info=process_results[1])

# DAG 인스턴스 생성
wconcept_dag_instance = wconcept_items_dag()
