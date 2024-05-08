class handsome_preprocess:
    def get_product(self, goods):
        product = {
            'product_id': goods['goodsNo'],
            'brand': goods['brandNm'],
            'fixed_price': goods['norPrc'],
            'discounted_price': goods['salePrc'],
            'url': self.extract_images(goods),
            'colors': self.extract_color(goods),
            'sizes': self.extract_size(goods),
            'review_count': goods['goodsRevCnt']
        }
        return product

    def get_product_info(self, soup, product_id):
        # product_info 요소가 있으면 그 값을 사용하고, 없으면 None을 할당
        product_info_elements = soup.select('div.prd-desc-box')
        product_info_text = product_info_elements[0].text if product_info_elements else None

        # fitting_info 요소가 있으면 그 값을 사용하고, 없으면 None을 할당
        fitting_info_elements = soup.select('p.cmp-font')
        fitting_info_text = fitting_info_elements[0].text if fitting_info_elements else None

        # 상품 정보 구조
        productInfo = {
            'product_id': product_id,
            'product_info': product_info_text,
            'fitting_info': fitting_info_text,
            'additional_info': self.extract_additional_info(soup)
        }

        return productInfo

    def get_review(self, reviews):
        review_list = []
        for review in reviews['payload']['revAllList']:
            # 키
            try:
                height = review['revPrfleList'][0]['mbrPrfleValNm']
            except:
                height = None
            # 평소 사이즈
            try:
                nor_size = review['revPrfleList'][1]['mbrPrfleValNm']
            except:
                nor_size = None

            data = {
                'product_id': review['goodsNo'],
                'review_id': review['revNo'],
                'rating': review['revScrVal'],
                'written_date': review['revWrtDtm'],
                'user_id': review['loginId'],
                'body': review['revCont'],
                'product_sku': {'color': review['goodsClorNm'], 'size': review['goodsSzNm']},
                'import_source': review['shopNm'],
                'user_height': height,
                'user_size': nor_size
            }
            review_list.append(data)
        return review_list

    def extract_images(self, goods):
        image_urls = []
        try:    
            for i in range(len(goods['colorInfo'])):
                for j in range(len(goods['colorInfo'][i]['colorContInfo'])):
                    colorContInfo = goods['colorInfo'][i]['colorContInfo'][j]['dispGoodsContUrl']
                    image_urls.append(colorContInfo)
        except:
            pass
        return image_urls
    
    
    def extract_color(self, goods):
        colors = []
        try:
            for color in goods['colorInfo']:
                colors.append(color['optnNm'])
        except:
            pass
        return colors
    
    
    def extract_size(self, goods):
        sizes = []
        for i in range(len(goods['colorInfo'][0]['colorSizeInfo'])):
            size_info = goods['colorInfo'][0]['colorSizeInfo'][i]['erpSzCd']
            sizes.append(size_info)
        return sizes

    def extract_additional_info(self, soup):
        try:
            additional_infos = soup.select('ul.cmp-list.list-dotType2.bottom6')
            additional_info_processed = []
            
            for info in additional_infos:
                additional_info_processed.append(info.text)
        except:
            additional_info_processed = None
        return additional_info_processed

    def get_rank_score(self, ranking, item_count):
        try:
            rank_score = 1 - ((ranking - 1) / (item_count - 1))
        except:
            rank_score = 1
            
        return rank_score
    
    def get_payload(self, max_item_counts, middle_category, gender):
        gender_type = self.discriminate_gender_type(gender)
        payload = {
            "custNo": "",
            "dateType": "daily",
            "domain": 'WOMEN',
            "genderType": gender_type,
            "depth1Code": "10101",
            "depth2Code": middle_category,
            "pageNo": 1,
            "pageSize": max_item_counts
            }
        
        return payload       
    
    def discriminate_gender_type(self, gender):
        if gender == 'men':
            return 'men'
        else:
            return 'women'