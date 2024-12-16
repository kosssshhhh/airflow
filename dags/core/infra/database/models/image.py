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
    style_id = Column(String(255), ForeignKey('style.style_id'), primary_key=True)
    mall_type_id = Column(Enum(MallType), ForeignKey('style.mall_type_id'), primary_key=True)
    url = Column(String(255))
    sequence = Column(Integer)
    __table_args__ = (
        ForeignKeyConstraint(
            ['style_id','mall_type_id'],
            ['style.style_id', 'style.mall_type_id']
        )
    )