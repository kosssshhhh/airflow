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
class LoadMusinsaBrandStyle(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.mall_type_id = MallType.MUSINSA.value
            
    def replace_percentage(self, value):
        if value is None:
            return None
        if isinstance(value, int):
            return value  # 정수는 그대로 반환
        return value.replace('%', '')

    def save_style(self, style_info_list, execution_date):
        session = self.SessionFactory()
        style = []
        try:
            for style_info in style_info_list:
                style.append({'style_id': style_info['style_id'],
                                      'style_num':style_info['style_num'],
                                        'fixed_price': style_info['fixed_price'],
                                        'style_name': style_info['style_name'],
                                        'brand': style_info['brand'],
                                        'discounted_price': style_info['discounted_price'],
                                        'likes':style_info['like'],
                                        'crawled_date': execution_date,
                                        'cumulative_sales': style_info['cumulative_sales'],
                                        'male_percentage': style_info['male_percentage'],
                                        'female_percentage':style_info['female_percentage'],
                                        'age_under_18':self.replace_percentage(style_info['under_18']),
                                        'age_19_to_23':self.replace_percentage(style_info['age_19_to_23']),
                                        'age_24_to_28':self.replace_percentage(style_info['age_24_to_28']),
                                        'age_29_to_33':self.replace_percentage(style_info['age_29_to_33']),
                                        'age_34_to_39':self.replace_percentage(style_info['age_34_to_39']),
                                        'age_over_40':self.replace_percentage(style_info['over_40'])
                                        })
            if style:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO musinsa_brand_style
                    (style_id, fixed_price, style_name, brand, discounted_price, crawled_date, likes, 
                    cumulative_sales, male_percentage, female_percentage, age_under_18, age_19_to_23, 
                    age_24_to_28, age_29_to_33, age_34_to_39, age_over_40, style_num)
                    VALUES (:style_id, :fixed_price, :style_name, :brand, :discounted_price, :crawled_date, :likes,
                            :cumulative_sales, :male_percentage, :female_percentage, :age_under_18, :age_19_to_23,
                            :age_24_to_28, :age_29_to_33, :age_34_to_39, :age_over_40, :style_num)
                """)
                session.execute(insert_ignore_sql, style)
                session.commit()
                self.log.info(f"Inserted {len(style)} new ranking.")
            else:
                self.log.info("No new ranking to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:   
            session.close()
        
    def execute(self, context: Context, style_info):
        execution_date = pendulum.parse(context['execution_date'].strftime('%Y-%m-%d')).date()
        self.save_style(style_info, execution_date)
        
