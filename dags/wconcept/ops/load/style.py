from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.category import Category
from datetime import date
import pendulum
import logging, traceback
from core.infra.database.enum import MallType
from core.infra.database.models.style import StyleRanking
from core.infra.database.models.wconcept import WConceptVariable
from core.infra.database.models.base import MySQLBase
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from sqlalchemy.dialects.mysql import insert
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

class LoadWConceptStyle(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.mall_type_id = MallType.WCONCEPT.value

    def save_style(self, style_id_list):
        session = self.SessionFactory()
        try:
            new_styles = [
                {'style_id': style_id, 'mall_type_id': self.mall_type_id}
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

    def save_style_variable(self, style_info_list):
        session = self.SessionFactory()
        try:
            new_variables = [
                {
                    'style_id': style_variable['style_id'],
                    'mall_type_id': self.mall_type_id,
                    'sold_out': style_variable['sold_out'] == '판매중',
                    'likes': style_variable['likes'],
                }
                for style_variable in style_info_list
            ]

            if new_variables:
                for new_variable in new_variables:
                    insert_stmt = insert(WConceptVariable).values(new_variable)
                    update_stmt = insert_stmt.on_duplicate_key_update(
                        sold_out=insert_stmt.inserted.sold_out,
                        likes=insert_stmt.inserted.likes
                    )
                    session.execute(update_stmt)

                session.commit()
                self.log.info(f"Upserted {len(new_variables)} variables.")
            else:
                self.log.info("No new variables were inserted.")

        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()

    def get_category_from_database(self):
        session = self.SessionFactory()
        try:
            man_category_mapping = {
                row.name: row.category_id
                for row in session.query(Category.category_id, Category.name)
                .filter(
                    and_(
                        Category.mall_type_id == self.mall_type_id,
                        Category.org_category_id.like('%M')
                    )
                ).all()
            }
            women_category_mapping = {
                row.name: row.category_id
                for row in session.query(Category.category_id, Category.name)
                .filter(
                    and_(
                        Category.mall_type_id == self.mall_type_id,
                        Category.org_category_id.like('%W')
                    )
                ).all()
            }
            return man_category_mapping, women_category_mapping
        except Exception as e:
            self.log.error(traceback.format_exc())
            raise AirflowException("데이터베이스에서 카테고리 조회 실패")

        finally:
            session.close()

    def save_style_ranking(self, style_info_list, execution_date):
        session = self.SessionFactory()
        style_ranking = []
        try:
            man_category_mapping, women_category_mapping = self.get_category_from_database()

            for style in style_info_list:
                style_ranking.append({
                    'style_id': style['style_id'],
                    'category_id': self.get_category_id(man_category_mapping, women_category_mapping, style),
                    'mall_type_id': self.mall_type_id,
                    'fixed_price': style['fixed_price'],
                    'rank_score': style['rank_score'],
                    'brand': style['brand'],
                    'style_name': style['style_name'],
                    'discounted_price': style['discounted_price'],
                    'monetary_unit': 'KRW',
                    'crawled_date': execution_date
                })

            if style_ranking:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO style_ranking
                    (style_id, mall_type_id, category_id, fixed_price, rank_score, style_name, brand, discounted_price, monetary_unit, crawled_date)
                    VALUES (:style_id, :mall_type_id, :category_id, :fixed_price, :rank_score, :style_name, :brand, :discounted_price, :monetary_unit, :crawled_date)
                """)
                session.execute(insert_ignore_sql, style_ranking)
                session.commit()
                self.log.info(f"Inserted {len(style_ranking)} new rankings.")
            else:
                self.log.info("No new rankings to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")
        finally:
            session.close()

    def get_category_id(self, man_category_mapping, women_category_mapping, style):
        medium_name = style['category_per_depth'][0]['medium_name']
        if medium_name == "남성":
            return man_category_mapping[style['category_per_depth'][0]['category_depthname3']]
        elif medium_name == "여성":
            return women_category_mapping[style['category_per_depth'][0]['category_depthname3']]
        else:
            self.log.error("남성 또는 여성의 카테고리가 아닙니다.", style)

    def save_style_category(self, style_info_list):
        session = self.SessionFactory()
        try:
            man_category_mapping, women_category_mapping = self.get_category_from_database()

            category_style = [
                {
                    'style_id': style['style_id'],
                    'mall_type_id': self.mall_type_id,
                    'category_id': self.get_category_id(man_category_mapping, women_category_mapping, style)
                }
                for style in style_info_list
            ]

            if category_style:
                insert_ignore_sql = text("""
                    INSERT IGNORE INTO category_style (style_id, mall_type_id, category_id)
                    VALUES (:style_id, :mall_type_id, :category_id)
                """)
                session.execute(insert_ignore_sql, category_style)
                session.commit()
            else:
                self.log.info("No new categories to insert.")
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")
        finally:
            session.close()

    # 기존 xcom_pull 대신 output을 받아서 처리
    def execute(self, context: Context, style_id_list, style_info):
        
        execution_date = pendulum.parse(context['execution_date'].strftime('%Y-%m-%d')).date()
        self.save_style(style_id_list)
        self.save_style_category(style_info)
        self.save_style_variable(style_info)
        self.save_style_ranking(style_info, execution_date)
