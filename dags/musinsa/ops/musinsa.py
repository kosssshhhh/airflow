import time
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import re
from requests.exceptions import Timeout

ITEMS_COUNT = 100

middle_category_nums = ['001006', '001004', '001005', '001010', '001002', '001003',
                        '001001', '001011', '001013', '001008', '002022', '002001',
                        '002002', '002025', '002017', '002003', '002020', '002019',
                        '002023', '002018', '002004', '002008', '002007', '002024',
                        '002009', '002013', '002012', '002016', '002021', '002014',
                        '002006', '002015', '003002', '003007', '003008', '003004',
                        '003009', '003005', '003010', '003011', '003006', '002006',
                        '002007', '002008', '022001', '022002', '022003']

# 사용자 에이전트
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}

timeout_settings = (60, 60)

def get_response(url, headers):

    # GET 요청
    response = requests.get(url, headers=headers, timeout=timeout_settings)
    
    soup = BeautifulSoup(response.text, 'lxml')
    time.sleep(0.5)

    return soup

def get_items(num):
    
    url = f'https://www.musinsa.com/categories/item/{num}?d_cat_cd={num}&brand=&list_kind=small&sort=sale_high&sub_sort=1d&page=1&display_cnt=90&exclusive_yn=&sale_goods=&timesale_yn=&ex_soldout=&plusDeliveryYn=&kids=&color=&price1=&price2=&shoeSizeOption=&tags=&campaign_id=&includeKeywords=&measure='
    
    product_links = []
    flag = 0
    page = 1
    while flag == 0:
        
        soup = get_response(url, headers)

        products = soup.select('a.img-block')
        
        for product in products:
            product_links.append(product['href'].replace('//', 'https://'))
            if len(product_links) == ITEMS_COUNT:
                flag = 1
                break

        page += 1
        url = f'https://www.musinsa.com/categories/item/{num}?d_cat_cd={num}&brand=&list_kind=small&sort=sale_high&sub_sort=1d&page={page}&display_cnt=90&exclusive_yn=&sale_goods=&timesale_yn=&ex_soldout=&plusDeliveryYn=&kids=&color=&price1=&price2=&shoeSizeOption=&tags=&campaign_id=&includeKeywords=&measure='

    return product_links
        
def get_item_info(item_url):
    soup = get_response(item_url, headers)
    info = soup.find_all('script', {'type':'text/javascript'})[15]
    info = info.string

    pattern = re.compile(r'window\.__MSS__\.product\.state = ({.*?});\s*$', re.DOTALL)
    match = pattern.search(info)
    info = match.group(1)
    
    return json.loads(info)


def extract_favorite_num(goodsNo):
    url = 'https://like.musinsa.com/like/api/v2/liketypes/goods/counts'
    data = {"relationIds":[str(goodsNo)]}
    response = requests.post(url, json=data)
    soup = BeautifulSoup(response.text, 'lxml')
    info = soup.string
    favorites = json.loads(info)['data']['contents']['items'][0]['count']

    return favorites

    
def extract_needs_info(item_info):
    # 무신사 상품번호
    goodsNo = item_info['goodsNo']

    # 브랜드 명명
    brand = item_info['brand']

    # 품번
    styleNo = item_info['styleNo']

    # 좋아요 수
    favorites = extract_favorite_num(goodsNo)

    # 무신사 판매가
    originPrice = item_info['goodsPrice']['originPrice']

    # 무신사 회원가
    memberPrice = item_info['goodsPrice']['minPrice']

    # 상품 이미지 : 리스트에 str로 저장. 필요하면 수정
    image_urls = []
    image_urls.append(item_info['thumbnailImageUrl'])
    goodsImages = item_info['goodsImages']
    for goodsImage in goodsImages:
        image_urls.append(goodsImage['imageUrl'])

    url = f'https://goods-detail.musinsa.com/goods/{goodsNo}/stat'
    response = requests.get(url, headers=headers, timeout=timeout_settings)
    add_data = response.json()
    
    # 누적 판매
    try:
        cumulative_sales = add_data['data']['purchase']['total']
    except:
        cumulative_sales = None

    # 나이
    try:
        ages = add_data['data']['purchase']['rates']
        ages = {key: f"{value}%" for key, value in ages.items()}
    except:
        ages = None

    # 성비
    try:
        male = int(add_data['data']['purchase']['male'])
        female = int(add_data['data']['purchase']['female'])
        total_count = male + female
        male_percentage = round((male / total_count) * 100, -1)
        female_percentage = round((female / total_count) * 100, -1)
        male_percentage = f"{male_percentage}%"
        female_percentage = f"{female_percentage}%"
    except:
        male_percentage = None
        female_percentage = None

    # 대분류
    categoryDepth1Title = item_info['category']['categoryDepth1Title']

    # 중분류
    categoryDepth2Title = item_info['category']['categoryDepth2Title']


    data = {
        'goodsNo': goodsNo,
        'brand': brand,
        'styleNo': styleNo,
        'favorites': favorites,
        'originPrice': originPrice,
        'memberPrice': memberPrice,
        'categoryDepth1Title': categoryDepth1Title,
        'categoryDepth2Title': categoryDepth2Title,
        'goodsImages': image_urls,
        'cumulative_sales': cumulative_sales,
        'ages': ages,
        'male_percentage': male_percentage,
        'female_percentage': female_percentage,   
    }

    return data


def main():
    all_product_links = []
    for middle_category_num in middle_category_nums:
        product_links = get_items(middle_category_num)
        all_product_links += product_links
        print(len(all_product_links), all_product_links[-1])
        if len(all_product_links) >= 10:
            break
    
    all_product_links = all_product_links[:10]  # 100개의 상품 링크만 유지

    items_list = []

    for item_url in all_product_links:
        item_info = get_item_info(item_url)
        needs_info = extract_needs_info(item_info)
        items_list.append(needs_info)
        print(needs_info)
    
    df = pd.DataFrame(items_list)
    print(df)
    return df

if __name__ == "__main__":
    df_items = main()