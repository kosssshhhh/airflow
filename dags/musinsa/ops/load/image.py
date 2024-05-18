from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from core.infra.database.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook


class LoadMusinsaImage(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = BaseHook.get_connection('mysql')
        self.db_url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(self.db_url, echo=True)
        self.SessionFactory = sessionmaker(bind=self.engine) 
        
    def save_images(self, product_image_url: dict):
        session = self.SessionFactory()
        try:
            image_list = []
            for product_id, urls in product_image_url.items():
                for url in urls:
                    sequence = int(url.split('/')[-1].split('.')[0])
                    image = {
                        'product_id' : product_id,
                        'mall_type' : 'MUSINSA',
                        'url' : url,
                        'sequence' : sequence
                    }
                    image_list.append(image)
                
            insert_query = """
                INSERT IGNORE INTO image (product_id, mall_type, url, sequence)
                VALUES (:product_id, :mall_type, :url, :sequence)
            """
            
            session.execute(insert_query, image_list)
            session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(f"Error inserting images: {e}")
        finally:
            session.close()
            
    def execute(self, context: Context):
        task_instance = context["task_instance"]
        product_image_url = task_instance.xcom_pull(task_ids="transform.images", key="product_image_url")
        self.save_images(product_image_url)
    