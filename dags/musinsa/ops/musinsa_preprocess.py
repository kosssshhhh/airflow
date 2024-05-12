import numpy as np
import re
import json

class musinsa_preprocess:
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
        return tasks.json()['data']['contents']['items'][0]['count']

    
    def processing_stat(self, tasks):
        try:
            cumulative_sales = tasks.json()['data']['purchase']['total']
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
