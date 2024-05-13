from sqlalchemy import (
    Enum,
    Text,
    Date,
    Float,
    Column,
    ForeignKey,
    Integer,
    String,
    ForeignKeyConstraint,
)
from sqlalchemy.orm import declarative_base
from database.enum import MallType
from database.base import Base

class Image(Base):
    __tablename__ = "image"
    image_id = Column(Integer, autoincrement=True, primary_key=True)
    product_id = Column(String(255), ForeignKey('product.product_id'), primary_key=True)
    mall_type = Column(Enum(MallType), ForeignKey('product.mall_type'), primary_key=True)
    url = Column(String(255))
    sequence = Column(Integer)
    __table_args__ = (
        ForeignKeyConstraint(
            ['product_id','mall_type'],
            ['product.product_id', 'product.mall_type']
        )
    )