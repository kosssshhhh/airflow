import numpy as np
import re
from bs4 import BeautifulSoup

class WconceptPreprocess:
    def get_style(self, tasks, style_fp):
        # 카테고리
        categories = tasks['category']
        category_per_depth = self.get_category_per_depth(categories)

        style = {
        'brand': tasks['brandNameKr'],
        'style_id': tasks['itemCd'],
        'likes': tasks['heartCnt'],
        'sold_out': tasks['statusName'],
        'fixed_price': tasks['customerPrice'],
        'discounted_price': style_fp,
        'style_name': tasks['itemName'],
        'category_per_depth': category_per_depth,
        'color': tasks['color1'],
        }   
        
        return style
    
    def get_category_per_depth(self, categories):
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
        return category_per_depth


    def get_rank_score(self, ranking, total_items_count):
        if total_items_count <= 1:
            rank_score = 1
        else:
            rank_score = 1 - ((ranking - 1) / (total_items_count - 1))
            
        return rank_score
    
    def get_payload(self, max_item_counts, middle_category, gender):
        payload = {
            "custNo": "",
            "dateType": "daily",
            "domain": 'WOMEN',
            "genderType": gender,
            "depth1Code": "10101",
            "depth2Code": middle_category,
            "pageNo": 1,
            "pageSize": max_item_counts
            }
        
        return payload       
        
    def get_style_and_style_info_list(self, tasks):
        style_list = []
        style_info_list = []
        
        styles_in_page= tasks['data']['content']
        
        for style in styles_in_page:
            style_list.append(style['itemCd'])
            style_info_list.append([style['itemCd'], style['finalPrice']])
        
        style_info_list = np.array(style_info_list)
        rankings = np.arange(1, len(style_info_list) + 1)
        total_items_count = np.full(len(style_info_list), len(style_info_list))
        
        style_info_list= np.column_stack((style_info_list, rankings, total_items_count))
        style_info_list = style_info_list.tolist()
        
        return style_list, style_info_list


    def parse_image(self, soup):
        imageURLs = []
        images = soup.select('ul#gallery > li > a > img')
        for image in images:
            imageURLs.append(f'https:{image['src']}')
            
        return imageURLs

    
class WconceptReviewPreprocess:
    def get_option(self, review_info):
        try:
            option = review_info.select('div.pdt_review_option > p')[0].text.strip()
            option = option.split(':')[1].strip()
        except:
            option = None
        return option
    
    def get_cust_size_info(self, review_info):
        try:
            cust_size_info = review_info.select('div.pdt_review_option > p')[1].text.strip()
            cust_size_info = cust_size_info.split(':')[1].strip()
        except:
            cust_size_info = None
        return cust_size_info
    
    def get_style_detail(self, index, soup):
        try:
            sku = soup.select('ul.product_review_evaluation')[index] 
            # 사이즈
            size = sku.select('ul.product_review_evaluation > li > div > em')[0].text

            # 색상
            color = sku.select('ul.product_review_evaluation > li > div > em')[1].text

            # 소재
            texture = sku.select('ul.product_review_evaluation > li > div > em')[2].text
            
        except:
            size, color, texture, sku = None, None, None, None
        return size,color,texture, sku
    
        
    def get_review_data(self, index, soup, style_id):
        # 구매 옵션과 사이즈정보 빼내기 용
        review_info = soup.select('div.pdt_review_info')[index]
        # 리뷰 id
        review_id = soup.select('div.product_review_reaction > div > button.link_txt.open-layer.open-pop_review_report')[index]['data-idxnum']
        
        # 구매 옵션
        option = self.get_option(review_info)

        # 사이즈 정보
        cust_size_info = self.get_cust_size_info(review_info)
              
        # 사이즈, 색상, 소재 빼내기 용
        size, color, texture, sku = self.get_style_detail(index, soup)
        
        # rating 정보
        rating_pct = soup.select('div.star-grade > strong[style]')[index]
        rating = re.findall(r'\d+', str(rating_pct))[0]
        rating = int(int(rating) / 20)

        data = {
            'style_id': style_id,
            'review_id': review_id,
            'purchase_option': option,
            'size_info': cust_size_info,
            'size': size,
            'color': color,
            'material': texture,
            'user_id': soup.select('p.product_review_info_right > em')[index].text,
            'written_time': soup.select('p.product_review_info_right > span')[index].text,
            'body': soup.select('p.pdt_review_text')[index].text.strip(),
            'rate': rating,
            'likes': soup.select('button.like.btn_review_recommend')[index].text
        }   

        return data
               
   