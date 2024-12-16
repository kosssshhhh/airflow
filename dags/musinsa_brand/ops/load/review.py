from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import date
import pendulum
import json, traceback
from core.infra.database.models.base import MySQLBase
from core.infra.database.models.review import ReviewStyle
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class LoadMusinsaBrandReview(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        

    def save_review(self, style_review_list, execution_date):
        session = self.SessionFactory()
        try:
            new_reviews = [
                {
                    'review_id':review['review_id'],
                    'style_id': review['style_id'],
                    'rate': review['rate'],
                    'written_date': review['review_date'],
                    'crawled_date' : execution_date,
                    'user_info': review['user_info'],
                    'body': review['body'],
                    'likes': review['likes']
                }
                for review in style_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO musinsa_brand_review 
                    (review_id, written_date, style_id, crawled_date, rate, user_info, body, likes)
                    VALUES (:review_id, :written_date, :style_id, :crawled_date, :rate, :user_info, :body, :likes)
                """)
                for review in new_reviews:
                    session.execute(insert_ignore_sql, review)
                session.commit()
                self.log.info(f"Inserted {len(new_reviews)} new musinsa reviews.")
            else:
                self.log.info("No new styles to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

        

    def execute(self, context: Context,style_review_list, execution_date):
        self.save_review(style_review_list, execution_date)