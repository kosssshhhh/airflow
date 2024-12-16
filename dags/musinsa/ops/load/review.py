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


class LoadMusinsaReview(BaseOperator):
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
                'mall_type_id': "JN1qnDZA",
                'crawled_date': execution_date
            } for review in style_review_list]
            

            insert_stmt = text("""
                INSERT IGNORE INTO review_style (org_review_id, style_id, mall_type_id, crawled_date)
                VALUES (:org_review_id, :style_id, :mall_type_id, :crawled_date)
            """)
            
            session.execute(insert_stmt, new_review_style)
            session.commit()
            self.log.info(f"Inserted {len(new_review_style)} new reviews.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

    def get_review_mapping_in_chunks(self, session, review_ids, chunk_size=10000):
        review_mapping = {}
        for i in range(0, len(review_ids), chunk_size):
            chunk = review_ids[i:i + chunk_size]
            chunk_mapping = {
                row.org_review_id: row.review_id
                for row in session.query(ReviewStyle.org_review_id, ReviewStyle.review_id)
                .filter(ReviewStyle.mall_type_id == 'JN1qnDZA', ReviewStyle.org_review_id.in_(chunk))
                .all()
            }
            review_mapping.update(chunk_mapping)
        return review_mapping
        

    def save_review(self, style_review_list):
        session = self.SessionFactory()
        try:
            # style_review_list에서 모든 review_id를 추출
            review_ids = [review['review_id'] for review in style_review_list]

            # Chunking 방식으로 review_mapping 가져오기
            review_mapping = self.get_review_mapping_in_chunks(session, review_ids)

            self.log.info(f"review_mapping length: {len(review_mapping)}")

            new_reviews = [
                {
                    'review_id': review_mapping[review['review_id']],
                    'style_id': review['style_id'],
                    'org_review_id': review['review_id'],
                    'rate': review['rate'],
                    'written_date': review['review_date'],
                    'user_info': review['user_info'],
                    'body': review['body'],
                    'likes': review['likes']
                }
                for review in style_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO musinsa_review 
                    (review_id, written_date, style_id, org_review_id, rate, user_info, body, likes)
                    VALUES (:review_id, :written_date, :style_id, :org_review_id, :rate, :user_info, :body, :likes)
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

        

    def execute(self, context: Context):
        task_instance = context["task_instance"]
        execution_date = pendulum.parse(context['execution_date'].strftime('%Y-%m-%d')).date()
        style_review_list = task_instance.xcom_pull(task_ids="fetch.styles.reviews", key="style_review")
        self.save_review_style(style_review_list, execution_date)
        self.save_review(style_review_list)