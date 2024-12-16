from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from datetime import date
import pendulum
from core.infra.database.models.base import MySQLBase
from core.infra.database.models.review import ReviewStyle
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import traceback
from airflow.exceptions import AirflowException

class LoadWConceptReview(BaseOperator):
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
                'mall_type_id': "l8WAu4fP",
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

            
        

    def save_review(self, style_review_list):
        session = self.SessionFactory()
        try:
            review_mapping = {
                row.org_review_id: row.review_id
                for row in session.query(ReviewStyle.org_review_id, ReviewStyle.review_id)
                .filter(ReviewStyle.mall_type_id == 'l8WAu4fP').all()
            }
            new_reviews = [
                {
                    'review_id': review_mapping[review['review_id']],
                    'style_id': review['style_id'],
                    'org_review_id': review['review_id'],
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
                for review in style_review_list
            ]

            if new_reviews:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO wconcept_review 
                    (review_id, style_id, org_review_id, rate, size_info, purchase_option, size, material, user_id, written_date, body, likes)
                    VALUES (:review_id, :style_id, :org_review_id, :rate, :size_info, :purchase_option, :size, :material, :user_id, :written_date, :body, :likes)
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
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

        

    # execute 메서드에서 직접 데이터를 인자로 받도록 수정
    def execute(self, context: Context, style_review_list, execution_date):
        self.save_review_style(style_review_list, execution_date)
        self.save_review(style_review_list)