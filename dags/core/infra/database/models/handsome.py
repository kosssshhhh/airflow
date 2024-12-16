from sqlalchemy import Enum, Boolean, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import MySQLBase

class HandsomeVariable(MySQLBase):
    __tablename__ = "handsome_variable"
    variable_id = Column(Integer, primary_key=True, autoincrement=True)
    style_id = Column(String(255), ForeignKey('style.style_id'))
    mall_type_id = Column(Enum(MallType), ForeignKey('style.mall_type_id'))
    style_info = Column(Text)
    fitting_info = Column(Text)
    additional_info = Column(Text)
    __table_args__ = (
        ForeignKeyConstraint(
            ['style_id', 'mall_type_id'],
            ['style.style_id', 'style.mall_type_id']
        ),
        UniqueConstraint('style_id', 'mall_type_id', name='_style_mall_uc'),
        {'extend_existing': True}
    )

class HandsomeReview(MySQLBase):
    __tablename__ = "handsome_review"
    review_id = Column(Integer, ForeignKey('ReviewProduct.review_id') ,primary_key=True)
    org_review_id = Column(String(255), unique=True)
    style_id = Column(String(255))
    rate = Column(Integer)
    written_date = Column(Date)
    user_id = Column(String(255))
    body = Column(Text)
    style_color = Column(String(255))
    style_size = Column(String(255))
    import_source = Column(String(255))
    user_height = Column(Integer)
    user_size = Column(Integer)
    __table_args__ = (
        {"extend_existing": True}
    )