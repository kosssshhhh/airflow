import logging
import numpy as np
import re
import json

logger = logging.getLogger(__name__)

class MusinsaPreprocess:
    def get_rank_score(self, ranking, total_items_count):
        return 1 - (ranking / total_items_count)


    def get_total_page_counts(self, soup):
        return int(soup.select('span.totalPagingNum')[0].text)


    def merge_rank_score(self, pd_list):
        pd_list = np.array(pd_list)
        
        rankings = np.arange(1, len(pd_list) + 1)
        total_items_count = np.full(len(pd_list), len(pd_list))
        rank_score = self.get_rank_score(rankings, total_items_count)
        
        pd_list = np.column_stack((pd_list, rank_score)).tolist()

        return pd_list


    def processing_info(self, product_json):
        data = {
            'product_id': product_json['goodsNo'],
            'brand': product_json['brand'],
            'product_num': product_json['styleNo'],
            'fixed_price': product_json['goodsPrice']['originPrice'],
            'discounted_price': product_json['goodsPrice']['minPrice'],
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


    def parse(self, soup):
        try:
            info = soup.find_all('script', {'type':'text/javascript'})[15]
        except:
            return
        info = info.string

        pattern = re.compile(r'window\.__MSS__\.product\.state = ({.*?});\s*$', re.DOTALL)
        match = pattern.search(info)
        info = match.group(1)
        
        return json.loads(info)


class MusinsaReviewPreprocess:
    def parse(self, soup, product_id, review_type):
        one_page_reviews = []
        reviews = soup.select('div.review-list')
        
        for review in reviews:
            review_dict = self.review_preprocessing(review, product_id)
            review_dict['review_type'] = review_type
            logger.info(f"review_dict : {review_dict}")
            one_page_reviews.append(review_dict)
            
        return one_page_reviews

    def review_preprocessing(self, review, product_id):
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
    
