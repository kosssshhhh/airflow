from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.base import MySQLBase
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import traceback
from airflow.exceptions import AirflowException

class LoadMusinsaBrandImage(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        
    def save_images(self, cdn_image_url: dict):
        session = self.SessionFactory()
        try:
            image_list = []
            for style_id, urls in cdn_image_url.items():
                for url in urls:
                    sequence = int(url.split('/')[-1].split('.')[0])
                    image = {
                        'style_id' : style_id,
                        'url' : url,
                        'sequence' : sequence
                    }
                    image_list.append(image)
                
            insert_query = """
                INSERT IGNORE INTO musinsa_brand_image (style_id, url, sequence)
                VALUES (:style_id, :url, :sequence)
            """
            
            session.execute(insert_query, image_list)
            session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(f"Error inserting images: {e}")
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            session.close()
            
    def execute(self, context: Context, cdn_image_url: dict):
        self.save_images(cdn_image_url)
    