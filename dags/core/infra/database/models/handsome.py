from sqlalchemy import Enum, Boolean, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import Base

class HandsomeVariable(Base):
    __tablename__ = "handsome_variable"
    variable_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(255), ForeignKey('product.product_id'))
    mall_type = Column(Enum(MallType), ForeignKey('product.mall_type'))
    product_info = Column(Text)
    fitting_info = Column(Text)
    additional_info = Column(Text)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id', 'mall_type'],
            ['product.product_id', 'product.mall_type']
        ),
        UniqueConstraint('product_id', 'mall_type', name='_product_mall_uc'),
        {'extend_existing': True}
    )

class HandsomeReview(Base):
    __tablename__ = "handsome_review"
    review_id = Column(Integer, ForeignKey('ReviewProduct.review_id') ,primary_key=True)
    org_review_id = Column(String(255), unique=True)
    product_id = Column(String(255))
    rating = Column(Integer)
    written_date = Column(Date)
    user_id = Column(String(255))
    body = Column(Text)
    product_color = Column(String(255))
    product_size = Column(String(255))
    import_source = Column(String(255))
    user_height = Column(Integer)
    user_size = Column(Integer)
    __table_args__ = (
        {"extend_existing": True}
    )