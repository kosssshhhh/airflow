from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.category import Category
from datetime import date
import logging, traceback
from core.infra.database.enum import MallType
from core.infra.database.models.product import ProductRanking
from core.infra.database.models.wconcept import WConceptVariable
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from sqlalchemy.dialects.mysql import insert

logger = logging.getLogger(__name__)
class LoadWConceptProduct(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        self.mall_type = MallType.WCONCEPT


    def save_product(self, product_id_list):
        session = self.SessionFactory()
        try:
            new_products = [
                {'product_id': product_id, 'mall_type': "WCONCEPT"}
                for product_id in product_id_list
            ]

            if new_products:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO product 
                    (product_id, mall_type)
                    VALUES (:product_id, :mall_type)
                """)
                session.execute(insert_ignore_sql, new_products)
                session.commit()
                self.log.info(f"Inserted {len(new_products)} new products.")
            else:
                self.log.info("No new products to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()

            
    def save_product_variable(self, product_info_list):
        session = self.SessionFactory()
        try:
            new_variables = [
                {
                    'product_id': product_variable['product_id'],
                    'mall_type': self.mall_type,
                    'product_name': product_variable['product_name'],
                    'sold_out': True if product_variable['sold_out'] == '판매중' else False,
                    'likes': product_variable['likes'],
                }
                for product_variable in product_info_list
            ]

            if new_variables:
                for new_variable in new_variables:
                    insert_stmt = insert(WConceptVariable).values(new_variable)
                    update_stmt = insert_stmt.on_duplicate_key_update(
                        product_name=insert_stmt.inserted.product_name,
                        sold_out=insert_stmt.inserted.sold_out,
                        likes=insert_stmt.inserted.likes
                    )
                    session.execute(update_stmt)

                session.commit()
                self.log.info(f"Upserted {len(new_variables)} variables.")
            else:
                self.log.info("No new variables were inserted")

        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
        finally:
            session.close()


    def save_product_ranking(self, product_info_list):
        session = self.SessionFactory()
        product_ranking = []
        try:
            category_mapping = {
                row.name: row.category_id
                for row in session.query(Category.category_id, Category.name)
                .filter(Category.mall_type == self.mall_type).all()
            }
            
            for product in product_info_list:
                product_ranking.append({'product_id': product['product_id'],
                                        'category_id':category_mapping[product['category_per_depth'][0]['category_depthname3']],
                                        'mall_type': self.mall_type,
                                        'fixed_price': product['fixed_price'],
                                        'rank_score': product['rank_score'],
                                        'brand': product['brand'],
                                        'discounted_price': product['discounted_price'],
                                        'monetary_unit': 'KRW',
                                        'crawled_date': date.today()})
            if product_ranking:
                session.bulk_insert_mappings(ProductRanking, product_ranking)
                session.commit()
                self.log.info(f"Inserted {len(product_ranking)} new ranking.")
            else:
                self.log.info("No new ranking to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:   
            session.close()
            
    def get_category_postfix(self, medium_category_name):
        if(medium_category_name == "남성"):
            return 'M'
        else:
            return 'W'

    def save_product_category(self, product_info_list):
        session = self.SessionFactory()
        try:
            # category 테이블에서 org_category_id와 category_id 매핑을 가져오기
            category_mapping = {
                row.name: row.category_id
                for row in session.query(Category.category_id, Category.name)
                .filter(Category.mall_type == self.mall_type).all()
            }

            category_product = []
            for product in product_info_list:
                org_category_name = product['category_per_depth'][0]['category_depthname3']
                product_id = product['product_id']

                # org_category_id가 매핑에 있는지 확인
                if org_category_name in category_mapping:
                    category_id = category_mapping[org_category_name]

                    category_product.append({
                        'product_id': product_id,
                        'mall_type': 'WCONCEPT',
                        'category_id': category_id
                    })

            if category_product:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO category_product (product_id, mall_type, category_id)
                    VALUES (:product_id, :mall_type, :category_id)
                """)

                result = session.execute(insert_ignore_sql, category_product)
                session.commit()
            else:
                self.log.info("No new categories to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.debug(f"Executed SQL: {insert_ignore_sql}")
            self.log.debug(f"Parameters: {category_product}")
        finally:
            session.close()

        
    def execute(self, context: Context):
        task_instance = context["task_instance"]
        product_info_list = task_instance.xcom_pull(task_ids="fetch.products.info", key="product_info")
        product_id_list = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        self.save_product(product_id_list)
        self.save_product_category(product_info_list)
        self.save_product_variable(product_info_list)
        self.save_product_ranking(product_info_list)
        
