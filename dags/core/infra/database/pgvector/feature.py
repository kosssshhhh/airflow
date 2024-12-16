from sqlalchemy import Column, String
from core.infra.database.models.base import PostgreSQLBase
from pgvector.sqlalchemy import Vector

class ImageVector(PostgreSQLBase):
    __tablename__ = "image_vector"
    style_id = Column(String(255))
    cdn_url = Column(String(255), primary_key=True, index=True)
    mall_type_id = Column(String(255))
    category = Column(String(255))
    embedding = Column(Vector(768))