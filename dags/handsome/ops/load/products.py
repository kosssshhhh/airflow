from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from core.infra.database.models.product import Product
# your_module은 모델이 정의된 모듈
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator

# 데이터베이스 엔진 설정 (SQLite 메모리 데이터베이스 사용)
engine = create_engine('mysql+pymysql://root:12341234@host.docker.internal/designoble_ex', echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)  # 모든 테이블 생성

class LoadHandsomeProductIds(BaseOperator): 
    def loadXCom(self, product_id_list):
        # 데이터베이스에 존재하는 모든 (product_id, mall_type) 조회
        existing_tuples = set(session.query(Product.product_id, Product.mall_type)
                            .filter(Product.mall_type == MallType.WCONCEPT)
                            .all())

        # 존재하지 않는 제품만 필터링
        new_products = [
            {'product_id': prod_id, 'mall_type': MallType.WCONCEPT}
            for prod_id in product_id_list
            if (prod_id, MallType.WCONCEPT) not in existing_tuples
        ]

        # 중복 제거 (새로운 제품 목록에서 중복 제거)
        unique_new_products = {tuple(prod.items()) for prod in new_products}
        unique_new_products = [dict(tup) for tup in unique_new_products]

        # bulk_insert_mappings 사용하여 새 제품들을 데이터베이스에 삽입
        if unique_new_products:
            session.bulk_insert_mappings(Product, unique_new_products)
            session.commit()
            print(f"Inserted {len(unique_new_products)} new products.")
        else:
            print("No new products to insert.")


    def execute(self, context: Context):
        task_instance = context["task_instance"]
        product_id_list = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        self.loadXCom(product_id_list)
        
    
