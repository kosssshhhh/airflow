from sqlalchemy import Enum, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base

class ReviewProduct(Base):
    __tablename__ = 'review_product'
    review_id = Column(Integer, primary_key=True, auto_increment=True)
    org_review_id = Column(String(255))
    product_id = Column(String(255), ForeignKey('product.product_id'))
    mall_type = Column(Enum(MallType), ForeignKey('product.mall_type'))
    crawled_date = Column(Date)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'mall_type'],
            ['product.product_id', 'product.mall_type']
        ),
        UniqueConstraint('org_review_id', 'mall_type'),  # 복합 유니크 제약 조건 추가
        {"extend_existing" : True}
    )