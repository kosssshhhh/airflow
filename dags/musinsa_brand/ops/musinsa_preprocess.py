import logging
import numpy as np
import re
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class MusinsaPreprocess:
    def get_rank_score(self, ranking, total_items_count):
        if len(total_items_count) <= 1:
            rank_score = 1
        else:
            rank_score = 1 - ((ranking - 1) / (total_items_count - 1))
            
        return rank_score


    def get_total_page_counts(self, soup):
        return int(soup.select('span.totalPagingNum')[0].text)


    def merge_rank_score(self, pd_list):
        pd_list = np.array(pd_list)
        
        rankings = np.arange(1, len(pd_list) + 1)
        total_items_count = np.full(len(pd_list), len(pd_list))
        rank_score = self.get_rank_score(rankings, total_items_count)
        
        pd_list = np.column_stack((pd_list, rank_score)).tolist()

        return pd_list


    def processing_info(self, style_json):
        data = {
            'style_id': style_json['goodsNo'],
            'style_name': style_json['goodsNm'],
            'brand': style_json['brand'],
            'style_num': style_json['styleNo'],
            'fixed_price': style_json['goodsPrice']['originPrice'],
            'discounted_price': style_json['goodsPrice']['minPrice'],
            'middle_category': style_json['category']['categoryDepth2Code']
        }

        return data

    def processing_like(self, tasks):
        return tasks['data']['contents']['items'][0]['count']

    
    def processing_stat(self, tasks):
        try:
            cumulative_sales = tasks['data']['purchase']['total']
        except:
            cumulative_sales = None
            
        try:
            ages = tasks['data']['purchase']['rates']
            ages = {key: f"{value}%" for key, value in ages.items()}
            under_18 = f"{ages['AGE_UNDER_18']}"
            age_19_to_23 = f"{ages['AGE_19_TO_23']}"
            age_24_to_28 = f"{ages['AGE_24_TO_28']}"
            age_29_to_33 = f"{ages['AGE_29_TO_33']}"
            age_34_to_39 = f"{ages['AGE_34_TO_39']}"
            over_40 = f"{ages['AGE_OVER_40']}"
        except:
            under_18, age_19_to_23, age_24_to_28, age_29_to_33, age_34_to_39, over_40 = None, None, None, None, None, None

        # 성비
        try:
            male = int(tasks['data']['purchase']['male'])
            female = int(tasks['data']['purchase']['female'])
            total_count = male + female
            male_percentage = int(round((male / total_count) * 100, -1))
            female_percentage = int(round((female / total_count) * 100, -1))
            male_percentage = male_percentage
            female_percentage = female_percentage
        except:
            male_percentage = None
            female_percentage = None
            
        return {
            'cumulative_sales': cumulative_sales,
            'under_18': under_18,
            'age_19_to_23': age_19_to_23,
            'age_24_to_28': age_24_to_28,
            'age_29_to_33': age_29_to_33,
            'age_34_to_39': age_34_to_39,
            'over_40': over_40,
            'male_percentage': male_percentage,
            'female_percentage': female_percentage
        }

    def processing_image(self, tasks):
        image_urls = []
        image_urls.append(f'https://image.msscdn.net{tasks['thumbnailImageUrl']}')
        goodsImages = tasks['goodsImages']
        
        for goodsImage in goodsImages:
            image_urls.append(f'https://image.msscdn.net{goodsImage['imageUrl']}')
            
        return image_urls
    
    
    def parse(self, soup):
        try:
            script_tag = soup.find('script', text=lambda t: t and 'window.__MSS__.product.state =' in t)

            # script 내용 중 JSON 객체가 시작되는 부분을 찾습니다.
            script_content = script_tag.string
            start_index = script_content.find('window.__MSS__.product.state =') + len('window.__MSS__.product.state =')
            end_index = script_content.find('};', start_index) + 1  # 객체의 끝부분을 찾습니다.

            json_str = script_content[start_index:end_index].strip()
            
            # JSON 문자열을 파이썬 딕셔너리로 변환합니다.
            product_info = json.loads(json_str)
        
        except:
            return

        # 결과 출력 (또는 다른 처리를 위해 반환할 수 있습니다)
        return json.loads(json.dumps(product_info, indent=4, ensure_ascii=False))



    # def parse(self, soup):
    #     try:
    #         info = soup.find_all('script', {'type':'text/javascript'})[2]
    #     except:
    #         return
    #     info = info.string

    #     pattern = re.compile(r'window\.__MSS__\.product\.state\s*=\s*(\{.*?\});', re.DOTALL)
    #     match = pattern.search(info)
    #     info = match.group(1)
        
    #     return json.loads(info)


class MusinsaReviewPreprocess:
    def parse(self, soup, style_id, review_type):
        one_page_reviews = []
        reviews = soup.select('div.review-list')
        
        for review in reviews:
            review_dict = self.review_preprocessing(review, style_id)
            review_dict['review_type'] = review_type
            one_page_reviews.append(review_dict)
            
        return one_page_reviews


    def process_date(self, date_str):
        now = datetime.now()
    
        days_match = re.search(r'(\d+)일 전', date_str)
        if days_match:
            days = int(days_match.group(1))
            processed_date = now - timedelta(days=days)
            return processed_date.strftime('%Y.%m.%d')
        
        hours_match = re.search(r'(\d+)시간 전', date_str)
        if hours_match:
            hours = int(hours_match.group(1))
            processed_date = now - timedelta(hours=hours)
            return processed_date.strftime('%Y.%m.%d')

        minute_match = re.search(r'(\d+)분 전', date_str)
        if minute_match:
            minutes = int(minute_match.group(1))
            processed_date = now - timedelta(minutes=minutes)
            
            return processed_date.strftime('%Y.%m.%d')
        
        return date_str


    

    def review_preprocessing(self, review, style_id):
        # 댓글 단 사람
        user_name = review.select('p.review-profile__name')[0].text

        # 리뷰 id
        review_id = review.select('div.review-contents')[0]['c_idx']

        # 리뷰 날짜
        review_date = self.process_date(review.select('p.review-profile__date')[0].text)
        
        # 메타 데이터
        meta_dict = {}
        metas = review.select('li.review-evaluation--type2__item')
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
        star = review['data-review-grade']

        data = {
            'style_id': style_id,
            'review_id': review_id,
            'review_date': review_date,
            'user_info': user_name,
            'meta_data': meta_dict,
            'body': content,
            'helpful': helpful,
            'good_style': style_good,
            'rate': star
        }

        return data
    
class MusinsaReviewPreprocess2:
    def get_review(self, reviews, style_id):
        review_list = []
        
        for review in reviews['data']['list']:
            # 댓글 단 사람
            user_name = review['userProfileInfo']['userNickName']
            
            # # 상품 id
            # style_id = review['goods']['goodsNo']
            
            # 리뷰 id
            review_id = str(review['no'])
            
            # 리뷰 날짜
            review_time = datetime.fromisoformat(review['createDate'])
            review_date = review_time.strftime('%Y-%m-%d')
            
            # 리뷰 내용
            content = review['content']
            
            # 좋아요 수
            review_like = review['likeCount']
            
            # 별 개수
            star = review['grade']
            
            data = {
                'style_id': style_id,
                'review_id': review_id,
                'review_date': review_date,
                'user_info': user_name,
                'body': content,
                'likes': review_like,
                'rate': star
            }
            
            review_list.append(data)
            
        return review_list
        
    
