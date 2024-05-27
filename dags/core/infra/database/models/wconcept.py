from sqlalchemy import Enum, Boolean, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base

class WConceptVariable(Base):
    __tablename__ = "wconcept_variable"
    variable_id = Column(Integer, primary_key= True, autoincrement= True)
    product_id = Column(String(255), ForeignKey('product.product_id'))
    mall_type = Column(Enum(MallType), ForeignKey('product.mall_type'))
    product_name = Column(String(255))
    likes = Column(Integer)
    sold_out = Column(Boolean)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'mall_type'], ["product.product_id", "product.mall_type"]
        ),
        {"extend_existing": True}
    )

class WConceptReview(Base):
    __tablename__ = "wconcept_review"
    review_id = Column(Integer, ForeignKey('ReviewProduct.review_id') ,primary_key=True)
    org_review_id = Column(String(255), unique=True)
    product_id = Column(String(255))
    purchase_option = Column(String(255))
    size_info = Column(String(255))
    size = Column(String(255))
    material = Column(String(255))
    user_id = Column(String(255))
    written_date = Column(Date)
    body = Column(Text)
    rate = Column(Integer)
    likes = Column(Integer)
    __table_args__ = (
        {"extend_existing": True}
    )