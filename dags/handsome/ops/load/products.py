from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.product import Product
from core.infra.database.models.category import CategoryProduct
from core.infra.database.models.category import Category
from datetime import date
import json
from core.infra.database.enum import MallType
from core.infra.database.models.product import ProductRanking
from core.infra.database.models.handsome import HandsomeVariable
from core.infra.database.models.product import SKUAttribute
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook

class LoadHandsomeProduct(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql') 
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.mall_type = MallType.HANDSOME  

    def save_product_id(self, product_id_list):
        session = self.SessionFactory()
        try:
            new_products = [
                {'product_id': product_id, 'mall_type': "HANDSOME"}
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
            existing_tuples = set(row[0] for row in session.query(HandsomeVariable.product_id).all())
            
            new_variables = [
                {'product_id': product_variable['product_id'],
                 'mall_type': self.mall_type,
                 'product_info': product_variable.get('product_info', ''),
                 'fitting_info': product_variable.get('fitting_info',''), 
                 'additional_info': json.dumps(product_variable.get('additional_info', []), ensure_ascii=False)[1:-1]}
                for product_variable in product_info_list
                if(product_variable['product_id']) not in existing_tuples
            ]
            
            
            if new_variables:
                session.bulk_insert_mappings(HandsomeVariable, new_variables)
                session.commit()
                self.log.info(f"Inserted {len(new_variables)} new variables.")
            else:
                self.log.info("No new variables were inserted")
            
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()



    def save_sku_attribute(self, product_list):
        session = self.SessionFactory()
        try:
            sku_attribute_list = []
            for product in product_list:
                product_id = product['product_id']
                attributes = {
                    'sizes': product.get('sizes', []),
                    'colors': product.get('colors', [])
                }

                for attr_key, values in attributes.items():
                    for value in values:
                        existing_entry = session.query(SKUAttribute).filter(
                            and_(
                                SKUAttribute.product_id == product_id,
                                SKUAttribute.mall_type == self.mall_type,
                                SKUAttribute.attr_key == attr_key,
                                SKUAttribute.attr_value == value
                            )
                        ).first()

                        if not existing_entry:
                            sku_attribute_list.append({
                                'product_id': product_id,
                                'mall_type': self.mall_type,
                                'attr_key': attr_key,
                                'attr_value': value
                            })

            if sku_attribute_list:
                session.bulk_insert_mappings(SKUAttribute, sku_attribute_list)
                session.commit()
                self.log.info(f"Inserted {len(sku_attribute_list)} new attributes.")
            else:
                self.log.info("No new attributes to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()
            
    def save_product_ranking(self, product_info_list):
        session = self.SessionFactory()
        product_ranking = []
        try:
                        # category 테이블에서 org_category_id와 category_id 매핑을 가져옵니다.
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type == self.mall_type).all()
            }
            
            for product in product_info_list:
                product_ranking.append({'product_id': product['product_id'],
                                        'mall_type': self.mall_type,
                                        'category_id': category_mapping[product['smallCategory']],
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
            

    def save_product_category(self, product_info_list):
        session = self.SessionFactory()
        try:
            # category 테이블에서 org_category_id와 category_id 매핑을 가져옵니다.
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type == self.mall_type).all()
            }

            category_product = []
            for product in product_info_list:
                org_category_id = product['smallCategory']
                product_id = product['product_id']

                # org_category_id가 매핑에 있는지 확인
                if org_category_id in category_mapping:
                    category_id = category_mapping[org_category_id]
                    category_product.append({
                        'product_id': product_id,
                        'mall_type': 'HANDSOME',
                        'category_id': category_id
                    })

            if category_product:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO category_product (product_id, mall_type, category_id)
                    VALUES (:product_id, :mall_type, :category_id)
                """)

                session.execute(insert_ignore_sql, category_product)
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
        product_id_list = task_instance.xcom_pull(task_ids="fetch.products", key="product_id_list")
        product_list = task_instance.xcom_pull(task_ids="fetch.products", key="product_list")
        product_info_list = task_instance.xcom_pull(task_ids="fetch.products.info", key="product_info")
        self.save_product_id(product_id_list)
        self.save_product_category(product_list)
        self.save_product_variable(product_info_list)
        self.save_sku_attribute(product_list)
        self.save_product_ranking(product_info_list)
