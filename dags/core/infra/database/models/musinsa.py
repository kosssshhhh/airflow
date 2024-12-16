from sqlalchemy import Enum, Boolean, Text, Date, Float, Column, ForeignKey, Integer, String, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base
from core.infra.database.enum import MallType
from core.infra.database.models.base import MySQLBase


class MusinsaVariable(MySQLBase):
    __tablename__ = "musinsa_variable"
    variable_id = Column(Integer, autoincrement=True, primary_key=True)
    style_id = Column(String(255), ForeignKey("style.style_id"))
    mall_type_id = Column(Enum(MallType), ForeignKey("style.mall_type_id"))
    style_num = Column(String(255))
    male_percentage = Column(Integer)
    female_percentage = Column(Integer)
    likes = Column(Integer)
    cumulative_sales = Column(Integer)
    written_date = Column(Date)
    age_under_18 = Column(Integer)
    age_19_to_23 = Column(Integer)
    age_24_to_28 = Column(Integer)
    age_29_to_33 = Column(Integer)
    age_34_to_39 = Column(Integer)
    age_over_40 = Column(Integer)
    __table_args__ = (
        ForeignKeyConstraint(
            ["style_id", "mall_type_id"], ["style.style_id", "style.mall_type_id"]
        ),
        UniqueConstraint('style_id', 'mall_type_id', name='_style_mall_uc'),
        {"extend_existing": True},
    )


class MusinsaReview(MySQLBase):
    __tablename__ = "musinsa_review"
    review_id = Column(Integer, ForeignKey('ReviewProduct.review_id') ,primary_key=True)
    org_review_id = Column(String(255), unique=True)
    style_id = Column(String(255))
    rate = Column(Integer)
    review_type = Column(String(255))
    user_info = Column(String(255))
    meta_data = Column(String(255))
    body = Column(Text)
    helpful = Column(Integer)
    good_style = Column(Integer)
    __table_args__ = (
        {"extend_existing": True},
    )
