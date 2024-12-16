import traceback
from core.infra.database.models.review import ReviewStyle
from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.review import ReviewStyle
from datetime import datetime
import pendulum
import re
from core.infra.database.enum import MallType
from core.infra.database.models.handsome import HandsomeReview
from core.infra.database.models.base import MySQLBase
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

class LoadHandsomeReview(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 

    def save_review_style(self, style_review_list, execution_date):
        session = self.SessionFactory()
        try:

            new_review_style = [{
                'org_review_id': review['review_id'],
                'style_id': review['style_id'],
                'mall_type_id': 'FHyETFQN',
                'crawled_date': execution_date
            } for review in style_review_list]
            
            # RINSERT IGNORE 구현
            insert_stmt = text("""
                INSERT IGNORE INTO review_style (org_review_id, style_id, mall_type_id, crawled_date)
                VALUES (:org_review_id, :style_id, :mall_type_id, :crawled_date)
            """)
            
            session.execute(insert_stmt, new_review_style)
            session.commit()
            self.log.info(f"Inserted {len(new_review_style)} new reviews.")
            self.save_review(style_review_list);
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

            
    def save_review(self, style_review_list):
        session = self.SessionFactory()
        try:
            review_mapping = {
                row.org_review_id: row.review_id
                for row in session.query(ReviewStyle.org_review_id, ReviewStyle.review_id)
                .filter(ReviewStyle.mall_type_id == 'FHyETFQN').all()
            }
            self.log.info(f"Review Mapping: {review_mapping}")
            
            new_reviews = [
                {
                    'review_id': review_mapping[review['review_id']],
                    'style_id': review['style_id'],
                    'org_review_id': review['review_id'],
                    'rate': review['rating'],
                    'style_color': review['style_sku']['color'],
                    'style_size': review['style_sku']['size'],
                    'import_source': review['import_source'],
                    'body': review['body'],
                    'written_date': datetime.strptime(review['written_date'], '%Y.%m.%d').date(),
                    'user_id': review['user_id'],
                    'user_height': self.parse_height(review['user_height']),
                    'user_size': review['user_size']
                }
                for review in style_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO handsome_review 
                    (review_id, style_id, org_review_id, rate, style_color, style_size, import_source, body, written_date, user_id, user_height, user_size)
                    VALUES (:review_id, :style_id, :org_review_id, :rate, :style_color, :style_size, :import_source, :body, :written_date, :user_id, :user_height, :user_size)
                """)
                session.execute(insert_ignore_sql, new_reviews)
                session.commit()
                self.log.info(f"Inserted {len(new_reviews)} new handsome reviews (duplicates ignored).")
            else:
                self.log.info("No new reviews to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

    
    def parse_height(self, height_str):
        if height_str is None or height_str.lower() == 'cm':
            return None
        match = re.search(r'\d+', height_str)
        return int(match.group()) if match else None

            

    def execute(self, context: Context):
        task_instance = context["task_instance"]
        execution_date = pendulum.parse(context['execution_date'].strftime('%Y-%m-%d')).date()
        style_review_list = task_instance.xcom_pull(task_ids="fetch.styles.reviews", key="style_reviews")
        self.save_review_style(style_review_list, execution_date)