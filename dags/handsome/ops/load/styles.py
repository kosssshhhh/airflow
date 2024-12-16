from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.category import CategoryStyle
from core.infra.database.models.category import Category
from datetime import date
import pendulum
import json, traceback
from core.infra.database.enum import MallType
from core.infra.database.models.style import StyleRanking
from core.infra.database.models.handsome import HandsomeVariable
from core.infra.database.models.style import SKUAttribute
from core.infra.database.models.base import MySQLBase
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from sqlalchemy.dialects.mysql import insert
from airflow.exceptions import AirflowException

class LoadHandsomeStyle(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql') 
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.mall_type_id = MallType.HANDSOME.value  

    def save_style_id(self, style_id_list):
        session = self.SessionFactory()
        try:
            new_styles = [
                {'style_id': style_id, 'mall_type_id': "FHyETFQN"}
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
                    'style_info': style_variable.get('style_info', ''),
                    'fitting_info': style_variable.get('fitting_info', ''),
                    'additional_info': json.dumps(style_variable.get('additional_info', []), ensure_ascii=False)[1:-1]
                }
                for style_variable in style_info_list
            ]

            if new_variables:
                for new_variable in new_variables:
                    insert_stmt = insert(HandsomeVariable).values(new_variable)
                    update_stmt = insert_stmt.on_duplicate_key_update(
                        style_info=insert_stmt.inserted.style_info,
                        fitting_info=insert_stmt.inserted.fitting_info,
                        additional_info=insert_stmt.inserted.additional_info
                    )
                    session.execute(update_stmt)

                session.commit()
                self.log.info(f"Upserted {len(new_variables)} variables.")
            else:
                self.log.info("No new variables were inserted")

        except Exception as e:
            session.rollback()
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()


    def save_sku_attribute(self, style_list):
        session = self.SessionFactory()
        try:
            sku_attribute_list = []
            for style in style_list:
                style_id = style['style_id']
                attributes = {
                    'sizes': style.get('sizes', []),
                    'colors': style.get('colors', [])
                }

                for attr_key, values in attributes.items():
                    for value in values:
                        existing_entry = session.query(SKUAttribute).filter(
                            and_(
                                SKUAttribute.style_id == style_id,
                                SKUAttribute.mall_type_id == self.mall_type_id,
                                SKUAttribute.attr_key == attr_key,
                                SKUAttribute.attr_value == value
                            )
                        ).first()

                        if not existing_entry:
                            sku_attribute_list.append({
                                'style_id': style_id,
                                'mall_type_id': self.mall_type_id,
                                'attr_key': attr_key,
                                'attr_value': value
                            })

            if sku_attribute_list:
                session.bulk_insert_mappings(SKUAttribute, sku_attribute_list)
                session.commit()
                self.log.info(f"Inserted {len(sku_attribute_list)} new attributes.")
            else:
                self.log.info("No new attributes to insert.")
                self.log.error(traceback.format_exc())
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
            # category 테이블에서 org_category_id와 category_id 매핑을 가져옵니다.
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type_id == 'FHyETFQN').all()
            }

            for style in style_info_list:
                self.log.info(f"org_category_id inside for: {style['smallCategory']}")
                style_ranking.append({'style_id': style['style_id'],
                                        'mall_type_id': self.mall_type_id,
                                        'category_id': category_mapping[str(style['smallCategory'])],
                                        'fixed_price': style['fixed_price'],
                                        'rank_score': style['rank_score'],
                                        'style_name': style['style_name'],
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
            category_mapping = {
                row.org_category_id: row.category_id
                for row in session.query(Category.org_category_id, Category.category_id)
                .filter(Category.mall_type_id == 'FHyETFQN').all()
            }

            category_style = []
            for style in style_info_list:
                org_category_id = str(style['smallCategory'])
                style_id = style['style_id']

                # org_category_id가 매핑에 있는지 확인
                if org_category_id in category_mapping:
                    category_id = str(category_mapping[org_category_id])
                    category_style.append({
                        'style_id': style_id,
                        'mall_type_id': 'FHyETFQN',
                        'category_id': category_id
                    })

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
            self.log.error(f"Error occurred: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()
        
    def execute(self, context: Context):
        task_instance = context["task_instance"]
        execution_date = pendulum.parse(context['execution_date'].strftime('%Y-%m-%d')).date()
        style_id_list = task_instance.xcom_pull(task_ids="fetch.styles", key="style_id_list")
        style_list = task_instance.xcom_pull(task_ids="fetch.styles", key="style_list")
        style_info_list = task_instance.xcom_pull(task_ids="fetch.styles.info", key="style_info")
        self.save_style_id(style_id_list)
        self.save_style_category(style_list)
        self.save_style_variable(style_info_list)
        self.save_sku_attribute(style_list)
        self.save_style_ranking(style_info_list, execution_date)
