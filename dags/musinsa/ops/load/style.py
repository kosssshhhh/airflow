from unicodedata import category
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.style import Style
from core.infra.database.models.category import CategoryStyle
from core.infra.database.models.category import Category
from datetime import date
import pendulum
import logging, traceback
from core.infra.database.enum import MallType
from core.infra.database.models.style import StyleRanking
from core.infra.database.models.musinsa import MusinsaVariable
from core.infra.database.models.style import SKUAttribute
from core.infra.database.models.base import MySQLBase
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from sqlalchemy.dialects.mysql import insert
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)
class LoadMusinsaStyle(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.mall_type_id = MallType.MUSINSA.value

    def save_style(self, style_id_list):
        session = self.SessionFactory()
        try:
            new_styles = [
                {'style_id': style_id, 'mall_type_id': "JN1qnDZA"}
                for style_id in style_id_list
            ]

            if new_styles:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO style 
                    (style_id, mall_type_id)
                    VALUES (:style_id, :mall_type_id)
                """)
                session.execute(insert_ignore_sql, new_styles)
                session.commit()
                self.log.info(f"Inserted {len(new_styles)} new styles.")
            else:
                self.log.info("No new styles to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()
            
    def replace_percentage(self, value):
        if value is None:
            return None
        return value.replace('%', '')


    def save_style_variable(self, style_info_list):
        session = self.SessionFactory()
        try:
            new_variables = [
                {
                    'style_id': style_variable['style_id'],
                    'mall_type_id': self.mall_type_id,
                    'style_num': style_variable['style_num'],
                    'male_percentage': style_variable['male_percentage'],
                    'female_percentage': style_variable['female_percentage'],
                    'likes': style_variable['like'],
                    'cumulative_sales': style_variable['cumulative_sales'],
                    'age_under_18': self.replace_percentage(style_variable.get('under_18')),
                    'age_19_to_23': self.replace_percentage(style_variable.get('age_19_to_23')),
                    'age_24_to_28': self.replace_percentage(style_variable.get('age_24_to_28')),
                    'age_29_to_33': self.replace_percentage(style_variable.get('age_29_to_33')),
                    'age_34_to_39': self.replace_percentage(style_variable.get('age_34_to_39')),
                    'age_over_40': self.replace_percentage(style_variable.get('over_40'))
                }
                for style_variable in style_info_list
            ]

            if new_variables:
                insert_stmt = insert(MusinsaVariable).values(new_variables)
                update_stmt = insert_stmt.on_duplicate_key_update(
                    style_num=insert_stmt.inserted.style_num,
                    male_percentage=insert_stmt.inserted.male_percentage,
                    female_percentage=insert_stmt.inserted.female_percentage,
                    likes=insert_stmt.inserted.likes,
                    cumulative_sales=insert_stmt.inserted.cumulative_sales,
                    age_under_18=insert_stmt.inserted.age_under_18,
                    age_19_to_23=insert_stmt.inserted.age_19_to_23,
                    age_24_to_28=insert_stmt.inserted.age_24_to_28,
                    age_29_to_33=insert_stmt.inserted.age_29_to_33,
                    age_34_to_39=insert_stmt.inserted.age_34_to_39,
                    age_over_40=insert_stmt.inserted.age_over_40
                )
            session.execute(update_stmt)
            session.commit()
            self.log.info(f"Upserted {len(new_variables)} variables.")

        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()


    def save_style_ranking(self, style_info_list, execution_date):
        session = self.SessionFactory()
        style_ranking = []
        try:
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type_id == self.mall_type_id).all()
            }
            
            for style in style_info_list:
                style_ranking.append({'style_id': style['style_id'],
                                        'category_id': category_mapping[style['middle_category']],
                                        'mall_type_id': self.mall_type_id,
                                        'fixed_price': style['fixed_price'],
                                        'style_name': style['style_name'],
                                        'rank_score': style['rank_score'],
                                        'brand': style['brand'],
                                        'discounted_price': style['discounted_price'],
                                        'monetary_unit': 'KRW',
                                        'crawled_date': execution_date})
            if style_ranking:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO style_ranking
                    (style_id, mall_type_id, category_id, fixed_price, rank_score, style_name, brand, discounted_price, monetary_unit, crawled_date)
                    VALUES (:style_id, :mall_type_id, :category_id, :fixed_price, :rank_score, :style_name, :brand, :discounted_price, :monetary_unit, :crawled_date)
                """)
                session.execute(insert_ignore_sql, style_ranking)
                session.commit()
                self.log.info(f"Inserted {len(style_ranking)} new ranking.")
            else:
                self.log.info("No new ranking to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:   
            session.close()
            

    def save_style_category(self, style_info_list):
        session = self.SessionFactory()
        try:
            # category 테이블에서 org_category_id와 category_id 매핑을 가져옵니다.
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type_id == self.mall_type_id).all()
            }

            category_style = []
            for style in style_info_list:
                org_category_id = style['middle_category']
                style_id = style['style_id']

                # org_category_id가 매핑에 있는지 확인
                if org_category_id in category_mapping:
                    category_id = category_mapping[org_category_id]

                    category_style.append({
                        'style_id': style_id,
                        'mall_type_id': 'JN1qnDZA',
                        'category_id': category_id
                    })

            if category_style:
                # SQLAlchemy의 text를 사용하여 직접 SQL 실행
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO category_style (style_id, mall_type_id, category_id)
                    VALUES (:style_id, :mall_type_id, :category_id)
                """)

                # executemany를 사용하여 일괄 삽입
                session.execute(insert_ignore_sql, category_style)
                session.commit()
            else:
                self.log.info("No new categories to insert.")
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
        style_info_list = task_instance.xcom_pull(task_ids="fetch.styles.info", key="style_info")
        style_id_list = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        self.save_style(style_id_list)
        self.save_style_category(style_info_list)
        self.save_style_variable(style_info_list)
        self.save_style_ranking(style_info_list, execution_date)
        
