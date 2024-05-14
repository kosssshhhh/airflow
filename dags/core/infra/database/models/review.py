from sqlalchemy import Enum, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base

class ReviewProduct(Base):
    __tablename__ = 'review_product'
    review_id = Column(String(255), primary_key=True)
    product_id = Column(String(255), ForeignKey('product.product_id'), primary_key=True)
    mall_type = Column(Enum(MallType), ForeignKey('product.mall_type'), primary_key=True)
    crawled_date = Column(Date)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'mall_type'],
            ['product.product_id', 'product.mall_type']
        ),
        {"extend_existing" : True}
    )