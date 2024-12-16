from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import numpy as np
from airflow.utils.context import Context
from core.infra.database.pgvector.feature import ImageVector
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import requests, traceback

class LoadWConceptFeature(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # PostgreSQL 연결 설정
        postgres_conn = BaseHook.get_connection('postgres') 
        self.postgres_db_url = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
        self.postgres_engine = create_engine(self.postgres_db_url, echo=True)
        self.PostgresSessionFactory = sessionmaker(bind=self.postgres_engine)

    def call_embedding_api(self, image_url_list, category_list):
        url = "https://definite-openly-caribou.ngrok-free.app"
        payload = {
            "image_url_list": image_url_list,
            "category_list": category_list
        }
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            raise Exception(f"API 호출 실패: {response.text}")
        return response.json()["vectorized_response"]

    def transform_data(self, xcom_data, cdn_image_url):
        transformed_data = []
        default_category = "apparel"
        for style_id, urls in cdn_image_url.items():
            style_info = next((item for item in xcom_data if item["style_id"] == style_id), {})
            category_depthname3 = style_info.get("category_per_depth", [{}])[0].get("category_depthname3", default_category)
            embeddings = self.call_embedding_api(urls, [category_depthname3] * len(urls))
            self.log.info(f"{style_id}: Embedding Complete")
            for i, url in enumerate(urls):
                data = {
                    "style_id": style_id,
                    "cdn_url": url,
                    "category": category_depthname3,
                    "mall_type_id": "l8WAu4fP",
                    "embedding": embeddings[i]
                }
                transformed_data.append(data)
        self.log.info(f"len(transformed_data): {len(transformed_data)}")
        return transformed_data
        
    def save_features(self, feature_data):
        session = self.PostgresSessionFactory()
        try:
            # Convert the features to a format suitable for SQLAlchemy
            feature_objects = [
                ImageVector(
                    style_id=data['style_id'],
                    cdn_url=data['cdn_url'],
                    category=data['category'],
                    mall_type_id=data['mall_type_id'],
                    embedding=np.array(data['embedding'])
                )
                for data in feature_data
            ]

            # Add and commit the features to the database
            session.add_all(feature_objects)
            session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise AirflowException("An error occurred, marking task for retry.")

        finally:
            # Close the session
            session.close()
            
    def execute(self, context: Context, cdn_image_url, style_info):
        feature_data = self.transform_data(style_info, cdn_image_url)
        self.save_features(feature_data)