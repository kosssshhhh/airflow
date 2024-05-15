from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.review import ReviewProduct
from datetime import datetime
from datetime import date
import re
from core.infra.database.enum import MallType
from core.infra.database.models.handsome import HandsomeReview
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator


class LoadHandsomeReview(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_url = 'mysql+pymysql://root:12341234@host.docker.internal/designoble_ex'
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        Base.metadata.create_all(self.engine) 

    def save_review_product(self, product_review_list):
        session = self.SessionFactory()
        try:

            new_review_product = [{
                'review_id': review['review_id'],
                'product_id': review['product_id'],
                'mall_type': 'HANDSOME',
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
                    'rating': review['rating'],
                    'product_color': review['product_sku']['color'],
                    'product_size': review['product_sku']['size'],
                    'import_source': review['import_source'],
                    'body': review['body'],
                    'written_date': datetime.strptime(review['written_date'], '%Y.%m.%d').date(),
                    'user_id': review['user_id'],
                    'user_height': self.parse_height(review['user_height']),
                    'user_size': review['user_size']
                }
                for review in product_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO handsome_review 
                    (product_id, review_id, rating, product_color, product_size, import_source, body, written_date, user_id, user_height, user_size)
                    VALUES (:product_id, :review_id, :rating, :product_color, :product_size, :import_source, :body, :written_date, :user_id, :user_height, :user_size)
                """)
                session.execute(insert_ignore_sql, new_reviews)
                session.commit()
                self.log.info(f"Inserted {len(new_reviews)} new handsome reviews (duplicates ignored).")
            else:
                self.log.info("No new reviews to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
        finally:
            session.close()

    
    def parse_height(self, height_str):
        if height_str is None or height_str.lower() == 'cm':
            return None
        match = re.search(r'\d+', height_str)
        return int(match.group()) if match else None

            

    def execute(self, context: Context):
        task_instance = context["task_instance"]
        product_review_list = task_instance.xcom_pull(task_ids="fetch.products.reviews", key="product_reviews")
        self.save_review_product(product_review_list)
        self.save_review(product_review_list)