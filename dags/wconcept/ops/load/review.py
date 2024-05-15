from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.review import ReviewProduct
from datetime import datetime
from datetime import date
import json
import re
from core.infra.database.enum import MallType
from core.infra.database.models.wconcept import WConceptReview
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook

class LoadWConceptReview(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        Base.metadata.create_all(self.engine) 

    def save_review_product(self, product_review_list):
        session = self.SessionFactory()
        try:

            new_review_product = [{
                'review_id': review['review_id'],
                'product_id': review['product_id'],
                'mall_type': "WCONCEPT",
                'crawled_date': date.today()
            } for review in product_review_list]
            
            # RINSERT IGNORE 구현
            insert_stmt = text("""
                INSERT IGNORE INTO review_product (review_id, product_id, mall_type, crawled_date)
                VALUES (:review_id, :product_id, :mall_type, :crawled_date)
            """)
            
            session.execute(insert_stmt, new_review_product)
            session.commit()
            self.log.info(f"Inserted {len(new_review_product)} new reviews.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()

            
        

    def save_review(self, product_review_list):
        session = self.SessionFactory()
        try:
            new_reviews = [
                {
                    'product_id': review['product_id'],
                    'review_id': review['review_id'],
                    'rate': review['rate'],
                    'size_info': review['size_info'],
                    'purchase_option': review['purchase_option'],
                    'size': review['size'],
                    'material': review['material'],
                    'user_id': review['user_id'],
                    'written_date': datetime.strptime(review['written_time'], '%Y.%m.%d').date(),
                    'body': review['body'],
                    'likes': review['likes']
                }
                for review in product_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO wconcept_review 
                    (product_id, review_id, rate, size_info, purchase_option, size, material, user_id, written_date, body, likes)
                    VALUES (:product_id, :review_id, :rate, :size_info, :purchase_option, :size, :material, :user_id, :written_date, :body, :likes)
                """)
                for review in new_reviews:
                    session.execute(insert_ignore_sql, review)
                session.commit()
                self.log.info(f"Inserted {len(new_reviews)} new wconcept reviews.")
            else:
                self.log.info("No new reviews to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()

        

    def execute(self, context: Context):
        task_instance = context["task_instance"]
        product_review_list = task_instance.xcom_pull(task_ids="fetch.products.reviews", key="product_review")
        self.save_review_product(product_review_list)
        self.save_review(product_review_list)